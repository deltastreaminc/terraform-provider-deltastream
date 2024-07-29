// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package secret

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"text/template"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/sethvargo/go-retry"

	gods "github.com/deltastreaminc/go-deltastream"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
)

var _ resource.Resource = &SecretResource{}
var _ resource.ResourceWithConfigure = &SecretResource{}

func NewSecretResource() resource.Resource {
	return &SecretResource{}
}

type SecretResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type SecretResourceData struct {
	Name             types.String `tfsdk:"name"`
	Type             types.String `tfsdk:"type"`
	Description      types.String `tfsdk:"description"`
	AccessRegion     types.String `tfsdk:"access_region"`
	Owner            types.String `tfsdk:"owner"`
	StringValue      types.String `tfsdk:"string_value"`
	CustomProperties types.Map    `tfsdk:"custom_properties"`
	Status           types.String `tfsdk:"status"`
	CreatedAt        types.String `tfsdk:"created_at"`
	UpdatedAt        types.String `tfsdk:"updated_at"`
}

func (d *SecretResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Secret resource",

		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the Secret",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"type": schema.StringAttribute{
				Description: "Secret type. (Valid values: generic_string)",
				Required:    true,
				Validators:  []validator.String{stringvalidator.OneOf("generic_string")},
			},
			"description": schema.StringAttribute{
				Description: "Description of the Secret",
				Optional:    true,
			},
			"access_region": schema.StringAttribute{
				Description: "Region the secret will be used in",
				Required:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Secret",
				Optional:    true,
				Computed:    true,
				Validators:  util.IdentifierValidators,
			},
			"string_value": schema.StringAttribute{
				Description: "Secret value",
				Optional:    true,
			},
			"custom_properties": schema.MapAttribute{
				Description: "Custom properties of the Secret",
				ElementType: types.StringType,
				Optional:    true,
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

func (d *SecretResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *SecretResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_secret"
}

const createStatement = `CREATE SECRET "{{.Name}}" WITH( 
	'type' = {{.Type}}, 
	{{ if .Description }}'description' = '{{.Description}}',{{ end }}
	{{ if .SecretString }}'secret_string' = '{{.SecretString}}',{{ end }}
	{{ range $k, $v := .CustomProperties }}'{{$k}}' = '{{$v}}',{{ end }}
	'access_region' = "{{.AccessRegion}}"
);`

// Create implements resource.Resource.
func (d *SecretResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var secret SecretResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &secret)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !secret.Owner.IsNull() && !secret.Owner.IsUnknown() {
		roleName = secret.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	customProps := map[string]string{}
	if !secret.CustomProperties.IsNull() && !secret.CustomProperties.IsUnknown() {
		resp.Diagnostics.Append(secret.CustomProperties.ElementsAs(ctx, &customProps, false)...)
		if resp.Diagnostics.HasError() {
			return
		}
	}

	b := bytes.NewBuffer(nil)
	template.Must(template.New("").Parse(createStatement)).Execute(b, map[string]any{
		"Name":             secret.Name.ValueString(),
		"Type":             secret.Type.ValueString(),
		"AccessRegion":     secret.AccessRegion.ValueString(),
		"Description":      secret.Description.ValueString(),
		"SecretString":     secret.StringValue.ValueString(),
		"CustomProperties": customProps,
	})
	if _, err := conn.ExecContext(ctx, b.String()); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create secret", err)
		return
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		secret, err = d.updateComputed(ctx, conn, secret)
		if err != nil {
			var godsErr gods.ErrSQLError
			if errors.As(err, &godsErr) && godsErr.SQLCode == gods.SqlStateInvalidSecret {
				return err
			}
			return retry.RetryableError(err)
		}
		if secret.Status.ValueString() != "ready" {
			return retry.RetryableError(fmt.Errorf("secret never transitioned to ready"))
		}
		return nil
	}); err != nil {
		if _, derr := conn.ExecContext(ctx, `DROP SECRET "`+secret.Name.ValueString()+`";`); derr != nil {
			tflog.Error(ctx, "failed to clean up secret", map[string]any{
				"name":  secret.Name.ValueString(),
				"error": derr.Error(),
			})
		}

		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create secret", err)
		return
	}
	tflog.Info(ctx, "Secret created", map[string]any{"name": secret.Name.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, secret)...)
}

func (d *SecretResource) updateComputed(ctx context.Context, conn *sql.Conn, db SecretResourceData) (SecretResourceData, error) {
	rows, err := conn.QueryContext(ctx, `LIST SECRETS;`)
	if err != nil {
		return db, err
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var name string
		var status string
		var owner string
		var updatedAt time.Time
		var createdAt time.Time
		if err := rows.Scan(&name, &discard, &discard, &discard, &status, &owner, &createdAt, &updatedAt); err != nil {
			return db, err
		}
		if name == db.Name.ValueString() {
			db.Status = types.StringValue(status)
			db.Owner = types.StringValue(owner)
			db.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
			db.UpdatedAt = types.StringValue(updatedAt.Format(time.RFC3339))
			return db, nil
		}
	}
	return SecretResourceData{}, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidSecret}
}

func (d *SecretResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var secret SecretResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &secret)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !secret.Owner.IsNull() && !secret.Owner.IsUnknown() {
		roleName = secret.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP SECRET "%s";`, secret.Name.ValueString())); err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidSecret {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to drop secret", err)
			return
		}
	}
	tflog.Info(ctx, "Secret deleted", map[string]any{"name": secret.Name.ValueString()})
}

func (d *SecretResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "update not supported", fmt.Errorf("secret updates not supported"))
}

func (d *SecretResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var Secret SecretResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &Secret)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	Secret, err = d.updateComputed(ctx, conn, Secret)
	if err != nil {
		var godsErr gods.ErrSQLError
		if errors.As(err, &godsErr) && godsErr.SQLCode == gods.SqlStateInvalidSecret {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, Secret)...)
}
