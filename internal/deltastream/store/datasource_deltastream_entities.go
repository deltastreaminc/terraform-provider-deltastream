// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"fmt"
	"text/template"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
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
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "provider error", fmt.Errorf("invalid provider data"))
		return
	}

	d.cfg = cfg
}

func (d *EntitiesDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_entities"
}

type EntitiesDataSourceData struct {
	Store         types.String `tfsdk:"store"`
	ParentPath    types.List   `tfsdk:"parent_path"`
	ChildEntities types.List   `tfsdk:"child_entities"`
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
				ElementType: types.StringType,
			},
			"child_entities": schema.ListAttribute{
				Description: "Child entities",
				Computed:    true,
				ElementType: types.StringType,
			},
		},
	}
}

const listEntitiesStatement = `LIST ENTITIES 
	{{ if ne (len .ParentPath) 0 }}
	IN {{ range $index, $element := .ParentPath -}}
        {{- if $index}}.{{end -}}
        "{{$element}}"
    {{- end }}
	{{ end }}
	IN STORE "{{ .StoreName }}";
`

func (d *EntitiesDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	entityData := EntitiesDataSourceData{}
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

	parentPath := []string{}
	if !entityData.ParentPath.IsNull() && !entityData.ParentPath.IsUnknown() {
		resp.Diagnostics.Append(entityData.ParentPath.ElementsAs(ctx, &parentPath, false)...)
	}

	b := bytes.NewBuffer(nil)
	if err := template.Must(template.New("").Parse(listEntitiesStatement)).Execute(b, map[string]any{
		"StoreName":  entityData.Store.ValueString(),
		"ParentPath": parentPath,
	}); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list entities in store", err)
		return
	}

	rows, err := conn.QueryContext(ctx, b.String())
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list store entities", err)
		return
	}
	defer rows.Close()

	items := []string{}
	for rows.Next() {
		var name string
		var isLeaf bool
		if err := rows.Scan(&name, &isLeaf); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read topics", err)
			return
		}
		items = append(items, name)
	}

	var dg diag.Diagnostics
	entityData.ChildEntities, dg = types.ListValueFrom(ctx, types.StringType, items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &entityData)...)
}
