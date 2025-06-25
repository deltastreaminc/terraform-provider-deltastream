// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var _ datasource.DataSource = &EntityDataDataSource{}
var _ datasource.DataSourceWithConfigure = &EntityDataDataSource{}

func NewEntityDataDataSource() datasource.DataSource {
	return &EntityDataDataSource{}
}

type EntityDataDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *EntityDataDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	cfg, ok := req.ProviderData.(*config.DeltaStreamProviderCfg)
	if !ok {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "provider error", fmt.Errorf("invalid provider data"))
		return
	}

	d.cfg = cfg
}

func (d *EntityDataDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_entity_data"
}

type EntityDataDataSourceData struct {
	Store         types.String `tfsdk:"store"`
	EntityPath    types.List   `tfsdk:"entity_path"`
	NumRows       types.Int64  `tfsdk:"num_rows"`
	FromBeginning types.Bool   `tfsdk:"from_beginning"`
	Rows          types.List   `tfsdk:"rows"`
}

func (d *EntityDataDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Entities in a store",

		Attributes: map[string]schema.Attribute{
			"store": schema.StringAttribute{
				Description: "Name of the Store",
				Required:    true,
			},
			"entity_path": schema.ListAttribute{
				Description: "Path to entity",
				Required:    true,
				ElementType: types.StringType,
			},
			"num_rows": schema.Int64Attribute{
				Description: "Number of rows to return",
				Optional:    true,
			},
			"from_beginning": schema.BoolAttribute{
				Description: "Read from beginning",
				Optional:    true,
			},
			"rows": schema.ListAttribute{
				Description: "Rows",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

func (d *EntityDataDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	entityData := EntityDataDataSourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &entityData)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	entityPath := []string{}
	if !entityData.EntityPath.IsNull() && !entityData.EntityPath.IsUnknown() {
		resp.Diagnostics.Append(entityData.EntityPath.ElementsAs(ctx, &entityPath, false)...)
	}

	dsql, err := util.ExecTemplate(printEntityTmpl, map[string]any{
		"StoreName":  entityData.Store.ValueString(),
		"EntityPath": entityPath,
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	rows, err := conn.QueryContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to print store entity", err)
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read columns", err)
		return
	}

	tflog.Info(ctx, "reading entity data")
	items := []string{}
	for rows.Next() {
		rowData := make([]any, len(cols))
		rowDataPtrs := make([]any, len(cols))
		for i := range rowData {
			rowDataPtrs[i] = &rowData[i]
		}

		if err := rows.Scan(rowDataPtrs...); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read entity data", err)
			return
		}

		b, err := json.Marshal(rowData)
		if err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to marshal entity data", err)
			return
		}
		items = append(items, string(b))

		if len(items) >= int(entityData.NumRows.ValueInt64()) {
			break
		}
	}

	var dg diag.Diagnostics
	entityData.Rows, dg = types.ListValueFrom(ctx, types.StringType, items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &entityData)...)
}
