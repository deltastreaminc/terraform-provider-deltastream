// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package region

import (
	"context"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

var _ datasource.DataSource = &RegionsDataSource{}
var _ datasource.DataSourceWithConfigure = &RegionsDataSource{}

func NewSecretsDataSources() datasource.DataSource {
	return &RegionsDataSource{}
}

type RegionsDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *RegionsDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *RegionsDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Regions resource",

		Attributes: map[string]schema.Attribute{
			"items": schema.ListNestedAttribute{
				Description: "List of regions",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: getRegionSchema().Attributes,
				},
			},
		},
	}
}

func (d *RegionsDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_regions"
}

type SecretsDatasourceData struct {
	Items basetypes.ListValue `tfsdk:"items"`
}

func (d *RegionsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	regions := SecretsDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &regions)...)
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

	rows, err := conn.QueryContext(ctx, `LIST REGIONS;`)
	if err != nil {
		resp.Diagnostics.AddError("failed to list region", err.Error())
		return
	}
	defer rows.Close()

	items := []RegionDataSourceData{}
	for rows.Next() {
		var name string
		var cloud string
		var region string
		if err := rows.Scan(&name, &cloud, &region); err != nil {
			resp.Diagnostics.AddError("failed to read region", err.Error())
			return
		}
		items = append(items, RegionDataSourceData{
			Name:   basetypes.NewStringValue(name),
			Cloud:  basetypes.NewStringValue(cloud),
			Region: basetypes.NewStringValue(region),
		})
	}

	var dg diag.Diagnostics
	regions.Items, dg = basetypes.NewListValueFrom(ctx, regions.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &regions)...)
}
