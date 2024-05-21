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
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
)

var _ datasource.DataSource = &SecretDataSource{}
var _ datasource.DataSourceWithConfigure = &SecretDataSource{}

func NewSecretDataSource() datasource.DataSource {
	return &SecretDataSource{}
}

type SecretDataSource struct {
	cfg *config.DeltaStreamProviderCfg
}

func (d *SecretDataSource) Configure(ctx context.Context, req datasource.ConfigureRequest, resp *datasource.ConfigureResponse) {
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

type SecretDatasourceData struct {
	Name         basetypes.StringValue `tfsdk:"name"`
	Type         basetypes.StringValue `tfsdk:"type"`
	Description  basetypes.StringValue `tfsdk:"description"`
	AccessRegion basetypes.StringValue `tfsdk:"access_region"`
	Owner        basetypes.StringValue `tfsdk:"owner"`
	Status       basetypes.StringValue `tfsdk:"status"`
	CreatedAt    basetypes.StringValue `tfsdk:"created_at"`
	UpdatedAt    basetypes.StringValue `tfsdk:"updated_at"`
}

func (d *SecretDataSource) Schema(ctx context.Context, req datasource.SchemaRequest, resp *datasource.SchemaResponse) {
	resp.Schema = getSecretSchema()
}

func getSecretSchema() schema.Schema {
	return schema.Schema{
		MarkdownDescription: "Secret resource",

		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the Secret",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"type": schema.StringAttribute{
				Description: "Secret type. (Valid values: generic_string)",
				Computed:    true,
			},
			"description": schema.StringAttribute{
				Description: "Description of the Secret",
				Computed:    true,
			},
			"access_region": schema.StringAttribute{
				Description: "Region the secret will be used in",
				Computed:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Secret",
				Computed:    true,
			},
			"status": schema.StringAttribute{
				Description: "Status of the Secret",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Last update date of the Secret",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the Secret",
				Computed:    true,
			},
		},
	}
}

func (d *SecretDataSource) Metadata(ctx context.Context, req datasource.MetadataRequest, resp *datasource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_secret"
}

func (d *SecretDataSource) Read(ctx context.Context, req datasource.ReadRequest, resp *datasource.ReadResponse) {
	secret := SecretDatasourceData{}
	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Config.Get(ctx, &secret)...)
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

	rows, err := conn.QueryContext(ctx, `LIST SECRETS;`)
	if err != nil {
		resp.Diagnostics.AddError("failed to list secrets", err.Error())
		return
	}
	defer rows.Close()

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
		if name == secret.Name.ValueString() {
			secret.Type = basetypes.NewStringValue(stype)
			secret.Description = basetypes.NewStringValue(description)
			secret.AccessRegion = basetypes.NewStringValue(region)
			secret.Status = basetypes.NewStringValue(status)
			secret.Owner = basetypes.NewStringValue(owner)
			secret.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			secret.UpdatedAt = basetypes.NewStringValue(updatedAt.Format(time.RFC3339))
			break
		}
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &secret)...)
}
