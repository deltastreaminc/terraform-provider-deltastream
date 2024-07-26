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
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
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
	Name basetypes.StringValue `tfsdk:"name"`
	// AccessRegion basetypes.StringValue `tfsdk:"access_region"`
	Type      basetypes.StringValue `tfsdk:"type"`
	Owner     basetypes.StringValue `tfsdk:"owner"`
	State     basetypes.StringValue `tfsdk:"state"`
	UpdatedAt basetypes.StringValue `tfsdk:"updated_at"`
	CreatedAt basetypes.StringValue `tfsdk:"created_at"`
}

type SchemaRegistriesDatasourceData struct {
	Items basetypes.ListValue `tfsdk:"items"`
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
						// "access_region": schema.StringAttribute{
						// 	Description: "Specifies the region of the schema registry",
						// 	Computed:    true,
						// },
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

	rows, err := conn.QueryContext(ctx, `LIST SCHEMA_REGISTRIES;`)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list schema registry", err)
		return
	}
	defer rows.Close()

	items := []SchemaRegistryDatasourceDataItem{}
	for rows.Next() {
		var discard any
		var name string
		// var accessRegion string
		var kind string
		var state string
		var owner string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &kind, &state, &discard, &owner, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read schema registry", err)
			return
		}
		items = append(items, SchemaRegistryDatasourceDataItem{
			Name:  basetypes.NewStringValue(name),
			Type:  basetypes.NewStringValue(kind),
			State: basetypes.NewStringValue(state),
			// AccessRegion: basetypes.NewStringValue(accessRegion),
			Owner:     basetypes.NewStringValue(owner),
			CreatedAt: basetypes.NewStringValue(createdAt.Format(time.RFC3339)),
			UpdatedAt: basetypes.NewStringValue(createdAt.Format(time.RFC3339)),
		})
	}

	var dg diag.Diagnostics
	schemaRegistries.Items, dg = basetypes.NewListValueFrom(ctx, schemaRegistries.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &schemaRegistries)...)
}
