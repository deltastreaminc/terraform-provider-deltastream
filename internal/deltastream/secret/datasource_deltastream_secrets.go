// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package secret

import (
	"context"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
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
		resp.Diagnostics.AddError("internal error", "invalid provider data")
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
	Items basetypes.ListValue `tfsdk:"items"`
}

func (d *SecretsDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	secrets := SecretsDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &secrets)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if err := util.SetSqlContext(ctx, d.cfg.Conn, &d.cfg.Role, nil, nil, nil); err != nil {
		resp.Diagnostics.AddError("failed to set sql context", err.Error())
		return
	}

	rows, err := d.cfg.Conn.QueryContext(ctx, `LIST SECRETS;`)
	if err != nil {
		resp.Diagnostics.AddError("failed to list secrets", err.Error())
		return
	}
	defer rows.Close()

	items := []SecretDatasourceData{}
	for rows.Next() {
		var name string
		var stype string
		var description string
		var region string
		var owner string
		var status string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &stype, &description, &region, &status, &owner, &createdAt, &updatedAt); err != nil {
			resp.Diagnostics.AddError("failed to read secret", err.Error())
			return
		}
		items = append(items, SecretDatasourceData{
			Name:         basetypes.NewStringValue(name),
			Type:         basetypes.NewStringValue(stype),
			Description:  basetypes.NewStringValue(description),
			AccessRegion: basetypes.NewStringValue(region),
			Owner:        basetypes.NewStringValue(owner),
			Status:       basetypes.NewStringValue(status),
			CreatedAt:    basetypes.NewStringValue(createdAt.Format(time.RFC3339)),
			UpdatedAt:    basetypes.NewStringValue(updatedAt.Format(time.RFC3339)),
		})
	}

	var dg diag.Diagnostics
	secrets.Items, dg = basetypes.NewListValueFrom(ctx, secrets.Items.ElementType(ctx), items)
	resp.Diagnostics.Append(dg...)

	resp.Diagnostics.Append(resp.State.Set(ctx, &secrets)...)
}