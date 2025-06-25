// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

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

var _ datasource.DataSource = &StoresDataSource{}
var _ datasource.DataSourceWithConfigure = &StoresDataSource{}

func NewStoresDataSource() datasource.DataSource {
	return &StoresDataSource{}
}

type StoresDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

type StoresDatasourceDataItem struct {
	Name      types.String `tfsdk:"name"`
	Type      types.String `tfsdk:"type"`
	Owner     types.String `tfsdk:"owner"`
	State     types.String `tfsdk:"state"`
	UpdatedAt types.String `tfsdk:"updated_at"`
	CreatedAt types.String `tfsdk:"created_at"`
}

type StoresDatasourceData struct {
	Items types.List `tfsdk:"items"`
}

func (d *StoresDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(listStoresTmpl, map[string]any{})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
	}
	rows, err := conn.QueryContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read stores", err)
	}
	defer rows.Close()

	var name string
	var kind string
	var state string
	var owner string
	var createdAt time.Time
	var updatedAt time.Time

	items := []StoresDatasourceDataItem{}
	for rows.Next() {
		if err := rows.Scan(&name, &kind, &state, &owner, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read stores", err)
			return
		}
		items = append(items, StoresDatasourceDataItem{
			Name:      types.StringValue(name),
			Type:      types.StringValue(kind),
			State:     types.StringValue(state),
			Owner:     types.StringValue(owner),
			CreatedAt: types.StringValue(createdAt.Format(time.RFC3339)),
			UpdatedAt: types.StringValue(createdAt.Format(time.RFC3339)),
		})
	}

	var dg diag.Diagnostics
	stores.Items, dg = types.ListValueFrom(ctx, stores.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &stores)...)
}
