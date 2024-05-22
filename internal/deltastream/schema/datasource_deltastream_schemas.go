// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package schema

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

var _ datasource.DataSource = &SchemasDataSource{}
var _ datasource.DataSourceWithConfigure = &SchemasDataSource{}

func NewSchemasDataSource() datasource.DataSource {
	return &SchemasDataSource{}
}

type SchemasDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *SchemasDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *SchemasDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Schema resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"items": schema.ListNestedAttribute{
				Description: "List of schemas",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: getSchemaSchema().Attributes,
				},
			},
		},
	}
}

func (d *SchemasDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schemas"
}

type SchemasDatasourceData struct {
	Database basetypes.StringValue `tfsdk:"database"`
	Items    basetypes.ListValue   `tfsdk:"items"`
}

func (d *SchemasDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	schemas := SchemasDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &schemas)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect", err.Error())
		return
	}
	defer conn.Close()

	if err := util.SetSqlContext(ctx, conn, &d.cfg.Role, nil, nil, nil); err != nil {
		resp.Diagnostics.AddError("failed to set sql context", err.Error())
		return
	}

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`LIST SCHEMAS IN DATABASE "%s";`, schemas.Database.ValueString()))
	if err != nil {
		resp.Diagnostics.AddError("failed to list schemas", err.Error())
		return
	}
	defer rows.Close()

	items := []SchemaDatasourceData{}
	for rows.Next() {
		var discard any
		var name string
		var owner string
		var createdAt time.Time
		if err := rows.Scan(&name, &discard, &owner, &createdAt); err != nil {
			resp.Diagnostics.AddError("failed to read schemas", err.Error())
			return
		}
		items = append(items, SchemaDatasourceData{
			Database:  schemas.Database,
			Name:      basetypes.NewStringValue(name),
			Owner:     basetypes.NewStringValue(owner),
			CreatedAt: basetypes.NewStringValue(createdAt.Format(time.RFC3339)),
		})
	}

	var dg diag.Diagnostics
	schemas.Items, dg = basetypes.NewListValueFrom(ctx, schemas.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &schemas)...)
}
