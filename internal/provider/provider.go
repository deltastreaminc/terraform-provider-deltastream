// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/function"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"

	gods "github.com/deltastreaminc/go-deltastream"
)

// Ensure ScaffoldingProvider satisfies various provider interfaces.
var _ provider.Provider = &DeltaStreamProvider{}
var _ provider.ProviderWithFunctions = &DeltaStreamProvider{}

// DeltaStreamProvider defines the provider implementation.
type DeltaStreamProvider struct {
	// version is the provider version. set by goreleaser.
	version string
}

// DeltaStreamProviderModel describes the provider data model.
type DeltaStreamProviderModel struct {
	APIKey             string  `tfsdk:"api_key"`
	Server             *string `tfsdk:"server"`
	InsecureSkipVerify *bool   `tfsdk:"insecure_skip_verify"`
	Organization       string  `tfsdk:"organization"`
	Role               string  `tfsdk:"role"`
}

func (p *DeltaStreamProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "deltastream"
	resp.Version = p.version
}

func (p *DeltaStreamProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Attributes: map[string]schema.Attribute{
			"api_key": schema.StringAttribute{
				Description: "API key",
				Required:    true,
			},
			"server": schema.StringAttribute{
				Description: "Server",
				Optional:    true,
			},
			"insecure_skip_verify": schema.BoolAttribute{
				Description: "Skip SSL verification",
				Optional:    true,
			},
			"organization": schema.StringAttribute{
				Description: "DeltaStream organization Name or ID",
				Required:    true,
			},
			"role": schema.StringAttribute{
				Description: "DeltaStream role to use for managing resources and queries",
				Required:    true,
			},
		},
	}
}

type DeltaStreamProviderCfg struct {
	Conn *sql.Conn
	Role string
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

	server := "https://api.deltastream.io/v2"
	if os.Getenv("DS_SERVER") != "" {
		server = os.Getenv("DS_SERVER")
	}
	if data.Server != nil {
		server = *data.Server
	}

	connOptions := []gods.ConnectionOption{gods.WithStaticToken(data.APIKey)}

	if data.InsecureSkipVerify != nil && *data.InsecureSkipVerify {

		httpClient := &http.Client{
			Transport: &debugTransport{
				r: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
				stderr: os.Stderr,
			},
		}
		connOptions = append(connOptions, gods.WithHTTPClient(httpClient))
	}

	connOptions = append(connOptions, gods.WithServer(server))
	connector, err := gods.ConnectorWithOptions(ctx, connOptions...)
	if err != nil {
		resp.Diagnostics.AddError("Failed to configure connection", err.Error())
		return
	}
	db := sql.OpenDB(connector)
	conn, err := db.Conn(ctx)
	if err != nil {
		resp.Diagnostics.AddError("Failed to configure connection", err.Error())
		return
	}

	if err := conn.PingContext(ctx); err != nil {
		resp.Diagnostics.AddError("Failed to establish connection", err.Error())
		return
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`USE ORGANIZATION "%s";`, data.Organization)); err != nil {
		resp.Diagnostics.AddError("Failed to set organization", err.Error())
		return
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, data.Role)); err != nil {
		resp.Diagnostics.AddError("Failed to set role", err.Error())
		return
	}

	resp.ResourceData = &DeltaStreamProviderCfg{
		Conn: conn,
		Role: data.Role,
	}
}

func (p *DeltaStreamProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewDatabaseResource,
		NewStoreResource,
		NewRelationResource,
		NewQueryResource,
	}
}

func (p *DeltaStreamProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}

func (p *DeltaStreamProvider) Functions(ctx context.Context) []func() function.Function {
	return []func() function.Function{}
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &DeltaStreamProvider{
			version: version,
		}
	}
}
