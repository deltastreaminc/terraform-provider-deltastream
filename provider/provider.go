// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"k8s.io/utils/ptr"

	gods "github.com/deltastreaminc/go-deltastream"
	"github.com/deltastreaminc/terraform-provider-deltastream/deltastream/database"
	dsnamespace "github.com/deltastreaminc/terraform-provider-deltastream/deltastream/namespace"
	dsobject "github.com/deltastreaminc/terraform-provider-deltastream/deltastream/object"
	"github.com/deltastreaminc/terraform-provider-deltastream/deltastream/query"
	schemaregistry "github.com/deltastreaminc/terraform-provider-deltastream/deltastream/schema_registry"
	"github.com/deltastreaminc/terraform-provider-deltastream/deltastream/secret"
	"github.com/deltastreaminc/terraform-provider-deltastream/deltastream/store"
	"github.com/deltastreaminc/terraform-provider-deltastream/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/util"
)

// Ensure ScaffoldingProvider satisfies various provider interfaces.
var _ provider.Provider = &DeltaStreamProvider{}

// DeltaStreamProvider defines the provider implementation.
type DeltaStreamProvider struct {
	// version is the provider version. set by goreleaser.
	version string
}

// DeltaStreamProviderModel describes the provider data model.
type DeltaStreamProviderModel struct {
	APIKey             types.String `tfsdk:"api_key"`
	Server             types.String `tfsdk:"server"`
	InsecureSkipVerify types.Bool   `tfsdk:"insecure_skip_verify"`
	Organization       types.String `tfsdk:"organization"`
	Role               types.String `tfsdk:"role"`
}

func (p *DeltaStreamProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "deltastream"
	resp.Version = p.version
}

func providerSchema() schema.Schema {
	return schema.Schema{
		Attributes: map[string]schema.Attribute{
			"api_key": schema.StringAttribute{
				Description: "API key. Can also be set via the DELTASTREAM_API_KEY environment variable",
				Optional:    true,
			},
			"server": schema.StringAttribute{
				Description: "Server. Can also be set via the DELTASTREAM_SERVER environment variable",
				Optional:    true,
			},
			"insecure_skip_verify": schema.BoolAttribute{
				Description: "Skip SSL verification",
				Optional:    true,
			},
			"organization": schema.StringAttribute{
				Description: "DeltaStream organization ID. Can also be set via the DELTASTREAM_ORGANIZATION environment variable.",
				Optional:    true,
				Validators:  util.UUIDValidators,
			},
			"role": schema.StringAttribute{
				Description: "DeltaStream role to use for managing resources and queries. Can also be set via the DELTASTREAM_ROLE environment variable. Default: sysadmin",
				Optional:    true,
			},
		},
	}
}

func (p *DeltaStreamProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = providerSchema()
}

type debugTransport struct {
	r         http.RoundTripper
	stderr    io.Writer
	sessionID *string
}

func (d *debugTransport) RoundTrip(h *http.Request) (*http.Response, error) {
	requestID := uuid.New().String()
	userAgent := "terraform-provider-deltastream request/" + requestID
	if d.sessionID != nil {
		userAgent += " session/" + *d.sessionID
	}
	h.Header.Set("User-Agent", userAgent)

	dump, _ := httputil.DumpRequestOut(h, true)
	fmt.Fprintf(d.stderr, "request (request %s) (session %s): %s\n", requestID, ptr.Deref(d.sessionID, ""), string(dump))
	resp, err := d.r.RoundTrip(h)
	if resp != nil {
		dump, _ = httputil.DumpResponse(resp, true)
		fmt.Fprintf(d.stderr, "response (request %s) (session %s): %s\n", requestID, ptr.Deref(d.sessionID, ""), string(dump))
	} else {
		fmt.Fprintf(d.stderr, "response is nil (request %s) (session %s)\n", requestID, ptr.Deref(d.sessionID, ""))
	}
	return resp, err
}

type httpTransport struct {
	r         http.RoundTripper
	sessionID *string
}

func (d *httpTransport) RoundTrip(h *http.Request) (*http.Response, error) {
	userAgent := "terraform-provider-deltastream"
	if d.sessionID != nil {
		userAgent += " session/" + *d.sessionID
	}
	h.Header.Set("User-Agent", userAgent)
	return d.r.RoundTrip(h)
}

func (p *DeltaStreamProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data DeltaStreamProviderModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	cfg := &config.DeltaStreamProviderCfg{
		Organization: os.Getenv("DELTASTREAM_ORGANIZATION"),
		Role:         os.Getenv("DELTASTREAM_ROLE"),
		SessionID:    ptr.To(os.Getenv("DELTASTREAM_SESSION_ID")),
	}
	apiKey := os.Getenv("DELTASTREAM_API_KEY")
	server := os.Getenv("DELTASTREAM_SERVER")
	debug := os.Getenv("DELTASTREAM_DEBUG") != ""
	insecureSkipVerify := os.Getenv("DELTASTREAM_INSECURE_SKIP_VERIFY") != ""

	if !data.Organization.IsNull() {
		cfg.Organization = data.Organization.ValueString()
	}
	if !data.Role.IsNull() {
		cfg.Role = data.Role.ValueString()
	}
	if !data.APIKey.IsNull() {
		apiKey = data.APIKey.ValueString()
	}
	if !data.Server.IsNull() {
		server = data.Server.ValueString()
	}

	if cfg.Organization == "" {
		resp.Diagnostics.AddAttributeError(path.Root("organization"), "Organization ID not specified", "Organization ID must be specified in the configuration or via the DELTASTREAM_ORGANIZATION environment variable")
	}
	if cfg.Role == "" {
		resp.Diagnostics.AddAttributeWarning(path.Root("role"), "Role not specified", "Role not specified in the configuration or via the DELTASTREAM_ORGANIZATION environment variable, defaulting to sysadmin")
		cfg.Role = "sysadmin"
	}
	if apiKey == "" {
		resp.Diagnostics.AddAttributeError(path.Root("api_key"), "API key not specified", "API key must be specified in the configuration or via the DELTASTREAM_API_KEY environment variable")
	}
	if server == "" {
		server = "https://api.deltastream.io/v2"
	}

	connOptions := []gods.ConnectionOption{gods.WithStaticToken(apiKey)}
	var sessionID *string
	if v := os.Getenv("DELTASTREAM_SESSION_ID"); v != "" {
		if v == "RANDOM" {
			v = uuid.NewString()
		}
		connOptions = append(connOptions, gods.WithSessionID(v))
		sessionID = ptr.To(v)
	}

	tlsConfig := &tls.Config{}
	if insecureSkipVerify {
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}

	t := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 20 * time.Second,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 1 * time.Minute,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       5 * time.Minute,
		TLSClientConfig:       tlsConfig,
		DisableKeepAlives:     true,
		MaxIdleConnsPerHost:   -1,
	}

	transport := http.RoundTripper(&httpTransport{
		r:         t,
		sessionID: sessionID,
	})

	if debug {
		transport = &debugTransport{
			r:         t,
			stderr:    os.Stderr,
			sessionID: sessionID,
		}
	}

	httpClient := &http.Client{
		Transport: transport,
	}

	connOptions = append(connOptions, gods.WithServer(server), gods.WithHTTPClient(httpClient))
	connector, err := gods.ConnectorWithOptions(ctx, connOptions...)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "Failed to configure connection", err)
		return
	}
	cfg.Db = sql.OpenDB(connector)

	if resp.Diagnostics.HasError() {
		return
	}

	resp.ResourceData = cfg
	resp.DataSourceData = cfg
}

func (p *DeltaStreamProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		database.NewDatabaseResource,
		dsnamespace.NewNamespaceResource,
		store.NewStoreResource,
		store.NewEntityResource,
		secret.NewSecretResource,
		dsobject.NewObjectResource,
		query.NewQueryResource,
		schemaregistry.NewSchemaRegistryResource,
	}
}

func (p *DeltaStreamProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		database.NewDatabaseDataSource,
		database.NewDatabasesDataSource,

		dsnamespace.NewNamespaceDataSource,
		dsnamespace.NewNamespacesDataSource,

		store.NewStoreDataSource,
		store.NewStoresDataSource,
		store.NewEntitiesDataSource,
		store.NewEntityDataDataSource,

		dsobject.NewObjectDataSource,
		dsobject.NewObjectsDataSource,

		secret.NewSecretDataSource,
		secret.NewSecretsDataSources,

		schemaregistry.NewSchemaRegistryDataSource,
		schemaregistry.NewSchemaRegistriesDataSource,
	}
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &DeltaStreamProvider{
			version: version,
		}
	}
}
