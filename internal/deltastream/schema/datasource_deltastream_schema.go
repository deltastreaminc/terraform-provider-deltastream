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
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

var _ datasource.DataSource = &SchemaDataSource{}
var _ datasource.DataSourceWithConfigure = &SchemaDataSource{}

func NewSchemaDataSource() datasource.DataSource {
	return &SchemaDataSource{}
}

type SchemaDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *SchemaDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

type SchemaDatasourceData = SchemaResourceData

func (d *SchemaDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = getSchemaSchema()
}

func getSchemaSchema() schema.Schema {
	return schema.Schema{
		MarkdownDescription: "Schema resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"name": schema.StringAttribute{
				Description: "Name of the Schema",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Schema",
				Optional:    true,
				Computed:    true,
				Validators:  util.IdentifierValidators,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the Schema",
				Computed:    true,
			},
		},
	}
}

func (d *SchemaDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schema"
}

func (d *SchemaDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	schema := SchemaDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &schema)...)
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

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`LIST SCHEMAS IN DATABASE "%s";`, schema.Database.ValueString()))
	if err != nil {
		resp.Diagnostics.AddError("failed to list schemas", err.Error())
		return
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var name string
		var owner string
		var createdAt time.Time
		if err := rows.Scan(&name, &discard, &owner, &createdAt); err != nil {
			resp.Diagnostics.AddError("failed to read database", err.Error())
			return
		}
		if name == schema.Name.ValueString() {
			schema.Owner = basetypes.NewStringValue(owner)
			schema.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			break
		}
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &schema)...)
}
