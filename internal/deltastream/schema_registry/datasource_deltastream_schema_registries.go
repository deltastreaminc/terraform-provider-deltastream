// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package schemaregistry

import (
	"context"
	"fmt"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var _ datasource.DataSource = &SchemaRegistriesDataSource{}
var _ datasource.DataSourceWithConfigure = &SchemaRegistriesDataSource{}

func NewSchemaRegistriesDataSource() datasource.DataSource {
	return &SchemaRegistriesDataSource{}
}

type SchemaRegistriesDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

type SchemaRegistryDatasourceDataItem struct {
	Name      types.String `tfsdk:"name"`
	Type      types.String `tfsdk:"type"`
	Owner     types.String `tfsdk:"owner"`
	State     types.String `tfsdk:"state"`
	UpdatedAt types.String `tfsdk:"updated_at"`
	CreatedAt types.String `tfsdk:"created_at"`
}

type SchemaRegistriesDatasourceData struct {
	Items types.List `tfsdk:"items"`
}

func (d *SchemaRegistriesDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	cfg, ok := req.ProviderData.(*config.DeltaStreamProviderCfg)
	if !ok {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "internal error", fmt.Errorf("invalid provider data"))
		return
	}

	d.cfg = cfg
}

func (d *SchemaRegistriesDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Schema registries datasource",

		Attributes: map[string]schema.Attribute{
			"items": schema.ListNestedAttribute{
				Description: "List of schema registries",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Description: "Name of the schema registry",
							Computed:    true,
						},
						"type": schema.StringAttribute{
							Description: "Type of the schema registry",
							Computed:    true,
						},
						"state": schema.StringAttribute{
							Description: "State of the schema registry",
							Computed:    true,
						},
						"owner": schema.StringAttribute{
							Description: "Owning role of the schema registry",
							Computed:    true,
						},
						"created_at": schema.StringAttribute{
							Description: "Creation date of the schema registry",
							Computed:    true,
						},
						"updated_at": schema.StringAttribute{
							Description: "Last update date of the schema registry",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

func (d *SchemaRegistriesDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schema_registries"
}

func (d *SchemaRegistriesDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	schemaRegistries := SchemaRegistriesDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &schemaRegistries)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(listSchemaRegistryTmpl, map[string]any{})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	rows, err := conn.QueryContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list schema registry", err)
		return
	}
	defer rows.Close()

	items := []SchemaRegistryDatasourceDataItem{}
	for rows.Next() {
		var name string
		var kind string
		var state string
		var owner string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &kind, &state, &owner, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read schema registry", err)
			return
		}
		items = append(items, SchemaRegistryDatasourceDataItem{
			Name:      types.StringValue(name),
			Type:      types.StringValue(kind),
			State:     types.StringValue(state),
			Owner:     types.StringValue(owner),
			CreatedAt: types.StringValue(createdAt.Format(time.RFC3339)),
			UpdatedAt: types.StringValue(createdAt.Format(time.RFC3339)),
		})
	}

	var dg diag.Diagnostics
	schemaRegistries.Items, dg = types.ListValueFrom(ctx, schemaRegistries.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &schemaRegistries)...)
}
