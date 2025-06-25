// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"context"
	"fmt"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var _ datasource.DataSource = &NamespacesDataSource{}
var _ datasource.DataSourceWithConfigure = &NamespacesDataSource{}

func NewNamespacesDataSource() datasource.DataSource {
	return &NamespacesDataSource{}
}

type NamespacesDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *NamespacesDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *NamespacesDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Namespace resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
			},
			"items": schema.ListNestedAttribute{
				Description: "List of namespaces",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: getNamespaceSchema().Attributes,
				},
			},
		},
	}
}

func (d *NamespacesDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namespaces"
}

type NamespacesDatasourceData struct {
	Database types.String `tfsdk:"database"`
	Items    types.List   `tfsdk:"items"`
}

func (d *NamespacesDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	namespaces := NamespacesDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &namespaces)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(listNamespacesTmpl, map[string]any{
		"DatabaseName": namespaces.Database.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	rows, err := conn.QueryContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list namespaces", err)
		return
	}
	defer rows.Close()

	items := []NamespaceDatasourceData{}
	for rows.Next() {
		var name string
		var owner string
		var createdAt time.Time
		if err := rows.Scan(&name, &owner, &createdAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read namespaces", err)
			return
		}
		items = append(items, NamespaceDatasourceData{
			Database:  namespaces.Database,
			Name:      types.StringValue(name),
			Owner:     types.StringValue(owner),
			CreatedAt: types.StringValue(createdAt.Format(time.RFC3339)),
		})
	}

	var dg diag.Diagnostics
	namespaces.Items, dg = types.ListValueFrom(ctx, namespaces.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &namespaces)...)
}
