// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

var _ datasource.DataSource = &StoresDataSource{}
var _ datasource.DataSourceWithConfigure = &StoresDataSource{}

func NewStoresDataSource() datasource.DataSource {
	return &StoresDataSource{}
}

type StoresDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

type StoresDatasourceDataItem struct {
	Name         basetypes.StringValue `tfsdk:"name"`
	AccessRegion basetypes.StringValue `tfsdk:"access_region"`
	Type         basetypes.StringValue `tfsdk:"type"`
	Owner        basetypes.StringValue `tfsdk:"owner"`
	State        basetypes.StringValue `tfsdk:"state"`
	UpdatedAt    basetypes.StringValue `tfsdk:"updated_at"`
	CreatedAt    basetypes.StringValue `tfsdk:"created_at"`
}

type StoresDatasourceData struct {
	Items basetypes.ListValue `tfsdk:"items"`
}

func (d *StoresDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	cfg, ok := req.ProviderData.(*config.DeltaStreamProviderCfg)
	if !ok {
		resp.Diagnostics.AddError("internal error", "invalid provider data")
		return
	}

	d.cfg = cfg
}

func (d *StoresDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Store resource",

		Attributes: map[string]schema.Attribute{
			"items": schema.ListNestedAttribute{
				Description: "List of stores",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"name": schema.StringAttribute{
							Description: "Name of the Store",
							Computed:    true,
						},
						"type": schema.StringAttribute{
							Description: "Type of the Store",
							Computed:    true,
						},
						"access_region": schema.StringAttribute{
							Description: "Specifies the region of the Store.",
							Computed:    true,
						},
						"state": schema.StringAttribute{
							Description: "State of the Store",
							Computed:    true,
						},
						"owner": schema.StringAttribute{
							Description: "Owning role of the Store",
							Computed:    true,
						},
						"created_at": schema.StringAttribute{
							Description: "Creation date of the Store",
							Computed:    true,
						},
						"updated_at": schema.StringAttribute{
							Description: "Last update date of the Store",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

func (d *StoresDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_stores"
}

func (d *StoresDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	stores := StoresDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &stores)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect to database", err.Error())
		return
	}
	defer conn.Close()

	if err := util.SetSqlContext(ctx, conn, &d.cfg.Role, nil, nil, nil); err != nil {
		resp.Diagnostics.AddError("failed to set sql context", err.Error())
		return
	}

	rows, err := conn.QueryContext(ctx, `LIST STORES;`)
	if err != nil {
		resp.Diagnostics.AddError("failed to list store", err.Error())
		return
	}
	defer rows.Close()

	items := []StoresDatasourceDataItem{}
	for rows.Next() {
		var discard any
		var name string
		var accessRegion string
		var kind string
		var state string
		var owner string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &kind, &accessRegion, &state, &discard, &owner, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics.AddError("failed to read stores", err.Error())
			return
		}
		items = append(items, StoresDatasourceDataItem{
			Name:         basetypes.NewStringValue(name),
			Type:         basetypes.NewStringValue(kind),
			AccessRegion: basetypes.NewStringValue(accessRegion),
			State:        basetypes.NewStringValue(state),
			Owner:        basetypes.NewStringValue(owner),
			CreatedAt:    basetypes.NewStringValue(createdAt.Format(time.RFC3339)),
			UpdatedAt:    basetypes.NewStringValue(createdAt.Format(time.RFC3339)),
		})
	}

	var dg diag.Diagnostics
	stores.Items, dg = basetypes.NewListValueFrom(ctx, stores.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &stores)...)
}
