// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package relation

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

var _ datasource.DataSource = &RelationsDataSource{}
var _ datasource.DataSourceWithConfigure = &RelationsDataSource{}

func NewRelationsDataSource() datasource.DataSource {
	return &RelationsDataSource{}
}

type RelationsDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *RelationsDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

type RelationsDataSourceData struct {
	Database  basetypes.StringValue `tfsdk:"database"`
	Schema    basetypes.StringValue `tfsdk:"schema"`
	Relations basetypes.ListValue   `tfsdk:"relations"`
}

func (d *RelationsDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Relation resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Name of the Schema",
				Required:    true,
			},
			"relations": schema.ListNestedAttribute{
				Description: "List of schemas",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"database": schema.StringAttribute{
							Description: "Name of the Database",
							Computed:    true,
						},
						"schema": schema.StringAttribute{
							Description: "Name of the Schema",
							Computed:    true,
						},
						"name": schema.StringAttribute{
							Description: "Name of the Schema",
							Computed:    true,
						},
						"fqn": schema.StringAttribute{
							Description: "Fully qualified name of the Relation",
							Computed:    true,
						},
						"owner": schema.StringAttribute{
							Description: "Owning role of the relation",
							Computed:    true,
						},
						"type": schema.StringAttribute{
							Description: "Type of the Relation",
							Computed:    true,
						},
						"state": schema.StringAttribute{
							Description: "State of the Relation",
							Computed:    true,
						},
						"created_at": schema.StringAttribute{
							Description: "Creation date of the relation",
							Computed:    true,
						},
						"updated_at": schema.StringAttribute{
							Description: "Creation date of the relation",
							Computed:    true,
						},
					},
				},
			},
		},
	}
}

func (d *RelationsDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_relations"
}

func (d *RelationsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	rels := RelationsDataSourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &rels)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if err := util.SetSqlContext(ctx, conn, &d.cfg.Role, nil, nil, nil); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`LIST RELATIONS IN SCHEMA "%s"."%s";`, rels.Database.ValueString(), rels.Schema.ValueString()))
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list schemas", err)
		return
	}
	defer rows.Close()

	relList := []RelationDataSourceData{}
	for rows.Next() {
		rel := RelationDataSourceData{
			Database: rels.Database,
			Schema:   rels.Schema,
		}
		var (
			name           string
			kind           string
			owner          string
			state          string
			propertiesJSON string
			createdAt      time.Time
			updatedAt      time.Time
		)

		if err := rows.Scan(&name, &kind, &owner, &state, &propertiesJSON, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read relation", err)
			return
		}
		rel.Name = basetypes.NewStringValue(name)
		rel.FQN = basetypes.NewStringValue(fmt.Sprintf("%s.%s.%s", rel.Database.ValueString(), rel.Schema.ValueString(), name))
		rel.Owner = basetypes.NewStringValue(owner)
		rel.Type = basetypes.NewStringValue(kind)
		rel.State = basetypes.NewStringValue(state)
		rel.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
		rel.UpdatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
		relList = append(relList, rel)
	}
	var dg diag.Diagnostics
	rels.Relations, dg = basetypes.NewListValueFrom(ctx, rels.Relations.ElementType(ctx), relList)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &rels)...)
}
