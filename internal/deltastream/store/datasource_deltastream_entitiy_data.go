// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"text/template"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
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
		resp.Diagnostics.AddError("provider error", "invalid provider data")
		return
	}

	d.cfg = cfg
}

func (d *EntityDataDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_entity_data"
}

type EntityDataDataSourceData struct {
	Store         basetypes.StringValue `tfsdk:"store"`
	EntityPath    basetypes.ListValue   `tfsdk:"entity_path"`
	NumRows       basetypes.Int64Value  `tfsdk:"num_rows"`
	FromBeginning basetypes.BoolValue   `tfsdk:"from_beginning"`
	Rows          basetypes.ListValue   `tfsdk:"rows"`
}

func (d *EntityDataDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Entities in a store",

		Attributes: map[string]schema.Attribute{
			"store": schema.StringAttribute{
				Description: "Name of the Store",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"entity_path": schema.ListAttribute{
				Description: "Path to entity",
				Required:    true,
				ElementType: basetypes.StringType{},
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
				ElementType: basetypes.StringType{},
			},
		},
	}
}

const printEntityStatement = `PRINT ENTITY
	{{ if ne (len .EntityPath) 0 }}
	{{- range $index, $element := .EntityPath }}
        {{- if $index }}.{{ end }}
    	{{- $element }}
    {{- end }}
	{{- end }}
	IN STORE {{ .StoreName }}
	{{ if .FromBeginning }}WITH ( 'from_beginning' ){{ end }};
`

func (d *EntityDataDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	entityData := EntityDataDataSourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &entityData)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect", err.Error())
		return
	}
	defer conn.Close()

	if err := util.SetSqlContext(ctx, conn, &d.cfg.Role, nil, nil, nil); err != nil {
		resp.Diagnostics.AddError("failed to set sql context", err.Error())
		return
	}

	entityPath := []string{}
	if !entityData.EntityPath.IsNull() && !entityData.EntityPath.IsUnknown() {
		resp.Diagnostics.Append(entityData.EntityPath.ElementsAs(ctx, &entityPath, false)...)
	}

	b := bytes.NewBuffer(nil)
	if err := template.Must(template.New("").Parse(printEntityStatement)).Execute(b, map[string]any{
		"StoreName":  entityData.Store.ValueString(),
		"EntityPath": entityPath,
	}); err != nil {
		resp.Diagnostics.AddError("failed to print entities", err.Error())
		return
	}

	rows, err := conn.QueryContext(ctx, b.String())
	if err != nil {
		resp.Diagnostics.AddError("failed to print store entity", err.Error())
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		resp.Diagnostics.AddError("failed to read columns", err.Error())
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
			resp.Diagnostics.AddError("failed to read entity data", err.Error())
			return
		}

		b, err := json.Marshal(rowData)
		if err != nil {
			resp.Diagnostics.AddError("failed to marshal entity data", err.Error())
			return
		}
		items = append(items, string(b))

		if len(items) >= int(entityData.NumRows.ValueInt64()) {
			break
		}
	}

	var dg diag.Diagnostics
	entityData.Rows, dg = basetypes.NewListValueFrom(ctx, basetypes.StringType{}, items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &entityData)...)
}
