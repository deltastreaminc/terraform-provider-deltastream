// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package object

import (
	"context"
	"fmt"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var _ datasource.DataSource = &ObjectDataSource{}
var _ datasource.DataSourceWithConfigure = &ObjectDataSource{}

func NewObjectDataSource() datasource.DataSource {
	return &ObjectDataSource{}
}

type ObjectDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *ObjectDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

type ObjectDataSourceData struct {
	Database  types.String `tfsdk:"database"`
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`
	FQN       types.String `tfsdk:"fqn"`
	Owner     types.String `tfsdk:"owner"`
	Type      types.String `tfsdk:"type"`
	State     types.String `tfsdk:"state"`
	CreatedAt types.String `tfsdk:"created_at"`
	UpdatedAt types.String `tfsdk:"updated_at"`
}

func (d *ObjectDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Object resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
			},
			"namespace": schema.StringAttribute{
				Description: "Name of the Namespace",
				Required:    true,
			},
			"name": schema.StringAttribute{
				Description: "Name of the Object",
				Required:    true,
			},
			"fqn": schema.StringAttribute{
				Description: "Fully qualified name of the Object",
				Computed:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the object",
				Computed:    true,
			},
			"type": schema.StringAttribute{
				Description: "Type of the Object",
				Computed:    true,
			},
			"state": schema.StringAttribute{
				Description: "State of the Object",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the object",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Last update date of the object",
				Computed:    true,
			},
		},
	}
}

func (d *ObjectDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_object"
}

func (d *ObjectDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	obj := ObjectDataSourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &obj)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(lookupObjectTmpl, map[string]any{
		"DatabaseName":  obj.Database.ValueString(),
		"NamespaceName": obj.Namespace.ValueString(),
		"Name":          obj.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	row := conn.QueryRowContext(ctx, dsql)
	if err := row.Err(); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read object", err)
		return
	}

	var (
		kind      string
		fqn       string
		owner     string
		state     string
		createdAt time.Time
		updatedAt time.Time
	)
	if err := row.Scan(&kind, &fqn, &owner, &state, &createdAt, &updatedAt); err != nil {
	}
	obj.FQN = types.StringValue(fqn)
	obj.Owner = types.StringValue(owner)
	obj.Type = types.StringValue(kind)
	obj.State = types.StringValue(state)
	obj.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	obj.UpdatedAt = types.StringValue(createdAt.Format(time.RFC3339))

	resp.Diagnostics.Append(resp.State.Set(ctx, &obj)...)
}
