// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package region

import (
	"context"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

var _ datasource.DataSource = &RegionDataSource{}
var _ datasource.DataSourceWithConfigure = &RegionDataSource{}

func NewRegionDataSource() datasource.DataSource {
	return &RegionDataSource{}
}

type RegionDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *RegionDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

type RegionDataSourceData struct {
	Name   basetypes.StringValue `tfsdk:"name"`
	Cloud  basetypes.StringValue `tfsdk:"cloud"`
	Region basetypes.StringValue `tfsdk:"region"`
}

func (d *RegionDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = getRegionSchema()
}

func getRegionSchema() schema.Schema {
	return schema.Schema{
		MarkdownDescription: "Region resource",

		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the Region",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"cloud": schema.StringAttribute{
				Description: "Cloud provider of the Region",
				Computed:    true,
			},
			"region": schema.StringAttribute{
				Description: "Cloud provider region",
				Computed:    true,
			},
		},
	}
}

func (d *RegionDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_region"
}

func (d *RegionDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	dsRegion := RegionDataSourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &dsRegion)...)
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

	rows, err := conn.QueryContext(ctx, `LIST REGIONS;`)
	if err != nil {
		resp.Diagnostics.AddError("failed to list regions", err.Error())
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var cloud string
		var region string
		if err := rows.Scan(&name, &cloud, &region); err != nil {
			resp.Diagnostics.AddError("failed to read region", err.Error())
			return
		}
		if name == dsRegion.Name.ValueString() {
			dsRegion.Cloud = basetypes.NewStringValue(cloud)
			dsRegion.Region = basetypes.NewStringValue(region)
			break
		}
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &dsRegion)...)
}
