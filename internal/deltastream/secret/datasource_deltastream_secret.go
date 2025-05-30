// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package secret

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/datasource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types"
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
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "provider error", fmt.Errorf("invalid provider data"))
		return
	}

	d.cfg = cfg
}

type SecretDatasourceData struct {
	Name        types.String `tfsdk:"name"`
	Type        types.String `tfsdk:"type"`
	Description types.String `tfsdk:"description"`
	Owner       types.String `tfsdk:"owner"`
	Status      types.String `tfsdk:"status"`
	CreatedAt   types.String `tfsdk:"created_at"`
	UpdatedAt   types.String `tfsdk:"updated_at"`
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
			},
			"type": schema.StringAttribute{
				Description: "Secret type. (Valid values: generic_string)",
				Computed:    true,
			},
			"description": schema.StringAttribute{
				Description: "Description of the Secret",
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

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(lookupSecretTmpl, map[string]any{
		"Name": secret.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	row := conn.QueryRowContext(ctx, dsql)
	if err := row.Err(); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to list secrets", err)
		return
	}

	var stype string
	var description *string
	var owner string
	var status string
	var createdAt time.Time
	var updatedAt time.Time
	if err := row.Scan(&stype, &description, &status, &owner, &createdAt, &updatedAt); err != nil {
		if err == sql.ErrNoRows {
			resp.Diagnostics.AddError("error loading secret", "secret not found")
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read secret", err)
		return
	}

	secret.Type = types.StringValue(stype)
	secret.Description = types.StringPointerValue(description)
	secret.Status = types.StringValue(status)
	secret.Owner = types.StringValue(owner)
	secret.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	secret.UpdatedAt = types.StringValue(updatedAt.Format(time.RFC3339))

	resp.Diagnostics.Append(resp.State.Set(ctx, &secret)...)
}
