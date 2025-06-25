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
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

var _ datasource.DataSource = &ObjectsDataSource{}
var _ datasource.DataSourceWithConfigure = &ObjectsDataSource{}

func NewObjectsDataSource() datasource.DataSource {
	return &ObjectsDataSource{}
}

type ObjectsDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *ObjectsDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

type ObjectsDataSourceData struct {
	Database  types.String `tfsdk:"database"`
	Namespace types.String `tfsdk:"namespace"`
	Objects   types.List   `tfsdk:"objects"`
}

func (d *ObjectsDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Objects resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
			},
			"namespace": schema.StringAttribute{
				Description: "Name of the Namespace",
				Required:    true,
			},
			"objects": schema.ListNestedAttribute{
				Description: "List of objects",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: map[string]schema.Attribute{
						"database": schema.StringAttribute{
							Description: "Name of the Database",
							Computed:    true,
						},
						"namespace": schema.StringAttribute{
							Description: "Name of the Namespace",
							Computed:    true,
						},
						"name": schema.StringAttribute{
							Description: "Name of the Object",
							Computed:    true,
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
				},
			},
		},
	}
}

func (d *ObjectsDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_objects"
}

func (d *ObjectsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	objs := ObjectsDataSourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &objs)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(listObjectsTmpl, map[string]any{
		"DatabaseName":  objs.Database.ValueString(),
		"NamespaceName": objs.Namespace.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	rows, err := conn.QueryContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to load objects", err)
		return
	}
	defer rows.Close()

	var (
		name      string
		fqn       string
		kind      string
		owner     string
		state     string
		createdAt time.Time
		updatedAt time.Time
	)

	objList := []ObjectDataSourceData{}
	for rows.Next() {
		obj := ObjectDataSourceData{
			Database:  objs.Database,
			Namespace: objs.Namespace,
		}
		if err := rows.Scan(&name, &fqn, &kind, &owner, &state, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read object", err)
			return
		}

		obj.Name = types.StringValue(name)
		obj.FQN = types.StringValue(fqn)
		obj.Owner = types.StringValue(owner)
		obj.Type = types.StringValue(kind)
		obj.State = types.StringValue(state)
		obj.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
		obj.UpdatedAt = types.StringValue(createdAt.Format(time.RFC3339))
		objList = append(objList, obj)
	}

	var dg diag.Diagnostics
	objs.Objects, dg = basetypes.NewListValueFrom(ctx, objs.Objects.ElementType(ctx), objList)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &objs)...)
}
