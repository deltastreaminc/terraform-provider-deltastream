// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package database

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

var _ datasource.DataSource = &DatabasesDataSource{}
var _ datasource.DataSourceWithConfigure = &DatabasesDataSource{}

func NewDatabasesDataSource() datasource.DataSource {
	return &DatabasesDataSource{}
}

type DatabasesDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *DatabasesDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *DatabasesDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Database resource",

		Attributes: map[string]schema.Attribute{
			"items": schema.ListNestedAttribute{
				Description: "List of databases",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: getDatabaseSchema().Attributes,
				},
			},
		},
	}
}

func (d *DatabasesDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_databases"
}

type DatabasesDatasourceData struct {
	Items basetypes.ListValue `tfsdk:"items"`
}

func (d *DatabasesDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	databases := DatabasesDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &databases)...)
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

	rows, err := conn.QueryContext(ctx, `LIST DATABASES;`)
	if err != nil {
		resp.Diagnostics.AddError("failed to list databases", err.Error())
		return
	}
	defer rows.Close()

	items := []DatabaseDatasourceData{}
	for rows.Next() {
		var discard any
		var name string
		var owner string
		var createdAt time.Time
		if err := rows.Scan(&name, &discard, &owner, &createdAt); err != nil {
			resp.Diagnostics.AddError("failed to read database", err.Error())
			return
		}
		items = append(items, DatabaseDatasourceData{
			Name:      basetypes.NewStringValue(name),
			Owner:     basetypes.NewStringValue(owner),
			CreatedAt: basetypes.NewStringValue(createdAt.Format(time.RFC3339)),
		})
	}

	var dg diag.Diagnostics
	databases.Items, dg = basetypes.NewListValueFrom(ctx, databases.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &databases)...)
}
