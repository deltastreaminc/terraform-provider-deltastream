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
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"k8s.io/utils/ptr"

	gods "github.com/deltastreaminc/go-deltastream"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/deltastream/database"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/deltastream/query"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/deltastream/region"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/deltastream/relation"
	dsschema "github.com/deltastreaminc/terraform-provider-deltastream/internal/deltastream/schema"
	schemaregistry "github.com/deltastreaminc/terraform-provider-deltastream/internal/deltastream/schema_registry"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/deltastream/secret"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/deltastream/store"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
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
	APIKey             *string `tfsdk:"api_key"`
	Server             *string `tfsdk:"server"`
	InsecureSkipVerify *bool   `tfsdk:"insecure_skip_verify"`
	Organization       *string `tfsdk:"organization"`
	Role               *string `tfsdk:"role"`
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
				Description: "Server. Can also be set via the DELTASTREAM_SERVER environment variable. Default: https://api.deltastream.io/v2",
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
				Validators:  util.IdentifierValidators,
			},
		},
	}
}

func (p *DeltaStreamProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = providerSchema()
}

type debugTransport struct {
	r      http.RoundTripper
	stderr io.Writer
}

func (d *debugTransport) RoundTrip(h *http.Request) (*http.Response, error) {
	dump, _ := httputil.DumpRequestOut(h, true)
	fmt.Fprintf(d.stderr, "request: %s\n", string(dump))
	resp, err := d.r.RoundTrip(h)
	if resp != nil {
		dump, _ = httputil.DumpResponse(resp, true)
		fmt.Fprintf(d.stderr, "response: %s\n", string(dump))
	} else {
		fmt.Fprintf(d.stderr, "response is nil\n")
	}
	return resp, err
}

func (p *DeltaStreamProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data DeltaStreamProviderModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	if osEnv := os.Getenv("DELTASTREAM_ORGANIZATION"); data.Organization == nil && osEnv != "" {
		data.Organization = ptr.To(osEnv)
	}
	if roleEnv := os.Getenv("DELTASTREAM_ROLE"); data.Role == nil && roleEnv != "" {
		data.Role = ptr.To(roleEnv)
	}
	if apiKeyEnv := os.Getenv("DELTASTREAM_API_KEY"); data.APIKey == nil && apiKeyEnv != "" {
		data.APIKey = ptr.To(apiKeyEnv)
	}
	if debug := os.Getenv("DELTASTREAM_DEBUG"); data.InsecureSkipVerify == nil && debug != "" {
		data.InsecureSkipVerify = ptr.To(true)
	}
	if insecure := os.Getenv("DELTASTREAM_INSECURE_SKIP_VERIFY"); data.InsecureSkipVerify == nil && insecure != "" {
		data.InsecureSkipVerify = ptr.To(true)
	}

	server := "https://api.deltastream.io/v2"
	if os.Getenv("DELTASTREAM_SERVER") != "" {
		server = os.Getenv("DELTASTREAM_SERVER")
	}
	if data.Server != nil {
		server = *data.Server
	}

	if data.APIKey == nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid configuration", fmt.Errorf("API key is required"))
		return
	}
	connOptions := []gods.ConnectionOption{gods.WithStaticToken(*data.APIKey)}
	var sessionID *string
	if v := os.Getenv("DELTASTREAM_SESSION_ID"); v != "" {
		if v == "RANDOM" {
			v = uuid.NewString()
		}
		connOptions = append(connOptions, gods.WithSessionID(v))
		sessionID = ptr.To(v)
	}

	transport := http.RoundTripper(&http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 20 * time.Second,
		}).Dial,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 1 * time.Minute,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       5 * time.Minute,
	})

	if data.InsecureSkipVerify != nil && *data.InsecureSkipVerify {
		transport = &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 20 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 1 * time.Minute,
			ExpectContinueTimeout: 1 * time.Second,
			IdleConnTimeout:       5 * time.Minute,
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		}
	}

	if debug := os.Getenv("DELTASTREAM_DEBUG"); debug != "" {
		transport = &debugTransport{
			r:      transport,
			stderr: os.Stderr,
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
	if data.Organization == nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid configuration", fmt.Errorf("organization is required"))
		return
	}
	db := sql.OpenDB(connector)

	resp.ResourceData = &config.DeltaStreamProviderCfg{
		Db:           db,
		Organization: *data.Organization,
		Role:         *data.Role,
		SessionID:    sessionID,
	}
	resp.DataSourceData = &config.DeltaStreamProviderCfg{
		Db:           db,
		Organization: *data.Organization,
		Role:         *data.Role,
		SessionID:    sessionID,
	}
}

func (p *DeltaStreamProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		database.NewDatabaseResource,
		dsschema.NewSchemaResource,
		store.NewStoreResource,
		store.NewEntityResource,
		secret.NewSecretResource,
		relation.NewRelationResource,
		query.NewQueryResource,
		schemaregistry.NewSchemaRegistryResource,
	}
}

func (p *DeltaStreamProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{
		database.NewDatabaseDataSource,
		database.NewDatabasesDataSource,

		dsschema.NewSchemaDataSource,
		dsschema.NewSchemasDataSource,

		region.NewRegionDataSource,
		region.NewSecretsDataSources,

		store.NewStoreDataSource,
		store.NewStoresDataSource,
		store.NewEntitiesDataSource,
		store.NewEntityDataDataSource,

		relation.NewRelationDataSource,
		relation.NewRelationsDataSource,

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
