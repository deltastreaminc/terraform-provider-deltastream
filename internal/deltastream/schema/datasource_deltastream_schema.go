// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
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
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "provider error", fmt.Errorf("invalid provider data"))
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
			},
			"name": schema.StringAttribute{
				Description: "Name of the Schema",
				Required:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Schema",
				Optional:    true,
				Computed:    true,
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

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(lookupSchemaTmpl, map[string]any{
		"DatabaseName": schema.Database.ValueString(),
		"Name":         schema.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	row := conn.QueryRowContext(ctx, dsql)
	if err := row.Err(); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to lookup schema", err)
		return
	}

	var owner string
	var createdAt time.Time
	if err := row.Scan(&owner, &createdAt); err != nil {
		if err == sql.ErrNoRows {
			resp.Diagnostics.AddError("error loading schema", "schema not found")
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read schema", err)
		return
	}

	schema.Owner = types.StringValue(owner)
	schema.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))

	resp.Diagnostics.Append(resp.State.Set(ctx, &schema)...)
}
