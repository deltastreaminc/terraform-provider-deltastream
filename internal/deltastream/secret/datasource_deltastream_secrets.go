// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package secret

import (
	"context"
	"fmt"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var _ datasource.DataSource = &SecretsDataSource{}
var _ datasource.DataSourceWithConfigure = &SecretsDataSource{}

func NewSecretsDataSources() datasource.DataSource {
	return &SecretsDataSource{}
}

type SecretsDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *SecretsDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

func (d *SecretsDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Secret resource",

		Attributes: map[string]schema.Attribute{
			"items": schema.ListNestedAttribute{
				Description: "List of secrets",
				Computed:    true,
				NestedObject: schema.NestedAttributeObject{
					Attributes: getSecretSchema().Attributes,
				},
			},
		},
	}
}

func (d *SecretsDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_secrets"
}

type SecretsDatasourceData struct {
	Items types.List `tfsdk:"items"`
}

func (d *SecretsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	secrets := SecretsDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &secrets)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(listSecretTmpl, map[string]any{})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	rows, err := conn.QueryContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list secrets", err)
		return
	}
	defer rows.Close()

	items := []SecretDatasourceData{}
	for rows.Next() {
		var name string
		var stype string
		var description *string
		var region string
		var owner string
		var status string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &stype, &description, &region, &status, &owner, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read secret", err)
			return
		}
		items = append(items, SecretDatasourceData{
			Name:         types.StringValue(name),
			Type:         types.StringValue(stype),
			Description:  types.StringPointerValue(description),
			AccessRegion: types.StringValue(region),
			Owner:        types.StringValue(owner),
			Status:       types.StringValue(status),
			CreatedAt:    types.StringValue(createdAt.Format(time.RFC3339)),
			UpdatedAt:    types.StringValue(updatedAt.Format(time.RFC3339)),
		})
	}

	var dg diag.Diagnostics
	secrets.Items, dg = types.ListValueFrom(ctx, secrets.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &secrets)...)
}
