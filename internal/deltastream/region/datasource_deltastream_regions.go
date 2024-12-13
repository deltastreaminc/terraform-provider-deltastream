// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package region

import (
	"context"
	"fmt"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
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
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "internal error", fmt.Errorf("invalid provider data"))
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
	Items types.List `tfsdk:"items"`
}

func (d *RegionsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	regions := SecretsDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &regions)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(lookupRegionsTmpl, map[string]any{})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	rows, err := conn.QueryContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list region", err)
		return
	}
	defer rows.Close()

	items := []RegionDataSourceData{}
	for rows.Next() {
		var name string
		var cloud string
		var region string
		if err := rows.Scan(&name, &cloud, &region); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read region", err)
			return
		}
		items = append(items, RegionDataSourceData{
			Name:   types.StringValue(util.ParseIdentifier(name)),
			Cloud:  types.StringValue(cloud),
			Region: types.StringValue(region),
		})
	}

	var dg diag.Diagnostics
	regions.Items, dg = types.ListValueFrom(ctx, regions.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &regions)...)
}
