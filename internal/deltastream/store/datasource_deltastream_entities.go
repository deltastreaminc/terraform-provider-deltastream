// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"text/template"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

var _ datasource.DataSource = &EntitiesDataSource{}
var _ datasource.DataSourceWithConfigure = &EntitiesDataSource{}

func NewEntitiesDataSource() datasource.DataSource {
	return &EntitiesDataSource{}
}

type EntitiesDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *EntitiesDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *EntitiesDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_entities"
}

type EntitiesDataSourceData struct {
	Store         basetypes.StringValue `tfsdk:"store"`
	ParentPath    basetypes.ListValue   `tfsdk:"parent_path"`
	ChildEntities basetypes.ListValue   `tfsdk:"child_entities"`
}

func (d *EntitiesDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Entities in a store",

		Attributes: map[string]schema.Attribute{
			"store": schema.StringAttribute{
				Description: "Name of the Store",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"parent_path": schema.ListAttribute{
				Description: "Path to parent entity",
				Optional:    true,
				ElementType: basetypes.StringType{},
			},
			"child_entities": schema.ListAttribute{
				Description: "Child entities",
				Computed:    true,
				ElementType: basetypes.StringType{},
			},
		},
	}
}

const listEntitiesStatement = `LIST ENTITIES 
	{{ if ne (len .ParentPath) 0 }}
	IN {{ range $index, $element := .ParentPath }}
        {{if $index}}.{{end}}
        {{$element}}
    {{ end }}
	{{ end }}
	IN STORE {{ .StoreName }};
`

func (d *EntitiesDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	entityData := EntitiesDataSourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &entityData)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := util.SetSqlContext(ctx, d.cfg.Conn, &d.cfg.Role, nil, nil, nil); err != nil {
		resp.Diagnostics.AddError("failed to set sql context", err.Error())
		return
	}

	parentPath := []string{}
	if !entityData.ParentPath.IsNull() && !entityData.ParentPath.IsUnknown() {
		resp.Diagnostics.Append(entityData.ParentPath.ElementsAs(ctx, &parentPath, false)...)
	}

	b := bytes.NewBuffer(nil)
	if err := template.Must(template.New("").Parse(listEntitiesStatement)).Execute(b, map[string]any{
		"StoreName":  entityData.Store.ValueString(),
		"ParentPath": parentPath,
	}); err != nil {
		resp.Diagnostics.AddError("failed to list entities in store", err.Error())
		return
	}
	if _, err := d.cfg.Conn.ExecContext(ctx, b.String()); err != nil {
		resp.Diagnostics.AddError("failed to list entities in store", err.Error())
		return
	}

	rows, err := d.cfg.Conn.QueryContext(ctx, b.String())
	if err != nil {
		resp.Diagnostics.AddError("failed to list store", err.Error())
		return
	}
	defer rows.Close()

	items := []string{}
	for rows.Next() {
		var name string
		var isLeaf bool
		if err := rows.Scan(&name, &isLeaf); err != nil {
			resp.Diagnostics.AddError("failed to read topics", err.Error())
			return
		}
		items = append(items, name)
	}

	var dg diag.Diagnostics
	entityData.ChildEntities, dg = basetypes.NewListValueFrom(ctx, basetypes.StringType{}, items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &entityData)...)
}
