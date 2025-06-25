// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var _ datasource.DataSource = &NamespaceDataSource{}
var _ datasource.DataSourceWithConfigure = &NamespaceDataSource{}

func NewNamespaceDataSource() datasource.DataSource {
	return &NamespaceDataSource{}
}

type NamespaceDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *NamespaceDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

type NamespaceDatasourceData = NamespaceResourceData

func (d *NamespaceDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = getNamespaceSchema()
}

func getNamespaceSchema() schema.Schema {
	return schema.Schema{
		MarkdownDescription: "Namespace resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
			},
			"name": schema.StringAttribute{
				Description: "Name of the Namespace",
				Required:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Namespace",
				Optional:    true,
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the Namespace",
				Computed:    true,
			},
		},
	}
}

func (d *NamespaceDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namespace"
}

func (d *NamespaceDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	namespace := NamespaceDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &namespace)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(lookupNamespaceTmpl, map[string]any{
		"DatabaseName": namespace.Database.ValueString(),
		"Name":         namespace.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	row := conn.QueryRowContext(ctx, dsql)
	if err := row.Err(); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to lookup namespace", err)
		return
	}

	var owner string
	var createdAt time.Time
	if err := row.Scan(&owner, &createdAt); err != nil {
		if err == sql.ErrNoRows {
			resp.Diagnostics.AddError("error loading namespace", "namespace not found")
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read namespace", err)
		return
	}

	namespace.Owner = types.StringValue(owner)
	namespace.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))

	resp.Diagnostics.Append(resp.State.Set(ctx, &namespace)...)
}
