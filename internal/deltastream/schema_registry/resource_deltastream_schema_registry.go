// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package schemaregistry

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"text/template"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/sethvargo/go-retry"

	gods "github.com/deltastreaminc/go-deltastream"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
)

var _ resource.Resource = &SchemaRegistryResource{}
var _ resource.ResourceWithConfigure = &SchemaRegistryResource{}

func NewSchemaRegistryResource() resource.Resource {
	return &SchemaRegistryResource{}
}

type SchemaRegistryResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type ConfluentProperties struct {
	Uris       basetypes.StringValue `tfsdk:"uris"`
	Username   basetypes.StringValue `tfsdk:"username"`
	Properties basetypes.StringValue `tfsdk:"password"`
}

type ConfluentCloudProperties struct {
	Uris   basetypes.StringValue `tfsdk:"uris"`
	Key    basetypes.StringValue `tfsdk:"key"`
	Secret basetypes.StringValue `tfsdk:"secret"`
}

type SchemaRegistryResourceData struct {
	Name           basetypes.StringValue `tfsdk:"name"`
	Type           basetypes.StringValue `tfsdk:"type"`
	AccessRegion   basetypes.StringValue `tfsdk:"access_region"`
	Confluent      basetypes.ObjectValue `tfsdk:"confluent"`
	ConfluentCloud basetypes.ObjectValue `tfsdk:"confluent_cloud"`
	Owner          basetypes.StringValue `tfsdk:"owner"`
	State          basetypes.StringValue `tfsdk:"state"`
	UpdatedAt      basetypes.StringValue `tfsdk:"updated_at"`
	CreatedAt      basetypes.StringValue `tfsdk:"created_at"`
}

func (d *SchemaRegistryResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Schema registry resource",

		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the schema registry",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"type": schema.StringAttribute{
				Description: "Type of the schema registry",
				Computed:    true,
			},
			"access_region": schema.StringAttribute{
				Description: "Region the schema registry will be used in",
				Required:    true,
			},
			"confluent": schema.SingleNestedAttribute{
				Description: "Confluent specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the schema registry",
						Required:    true,
					},
					"username": schema.StringAttribute{
						Description: "Username to use when authenticating with confluent schema registry",
						Optional:    true,
						Sensitive:   true,
					},
					"password": schema.StringAttribute{
						Description: "Password to use when authenticating with confluent schema registry",
						Optional:    true,
						Sensitive:   true,
					},
				},
				Optional: true,
			},
			"confluent_cloud": schema.SingleNestedAttribute{
				Description: "Confluent cloud specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the schema registry",
						Required:    true,
					},
					"key": schema.StringAttribute{
						Description: "Key to use when authenticating with confluent cloud schema registry",
						Optional:    true,
						Sensitive:   true,
					},
					"secret": schema.StringAttribute{
						Description: "Secret to use when authenticating with confluent cloud schema registry",
						Optional:    true,
						Sensitive:   true,
					},
				},
				Optional: true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the schema registry",
				Optional:    true,
				Computed:    true,
				Validators:  util.IdentifierValidators,
			},
			"state": schema.StringAttribute{
				Description: "Status of the schema registry",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Last update date of the schema registry",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the schema registry",
				Computed:    true,
			},
		},
	}
}

func (d *SchemaRegistryResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *SchemaRegistryResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schema_registry"
}

const createStatement = `CREATE SCHEMA_REGISTRY "{{.Name}}" WITH(
	{{- if eq .Type "CONFLUENT" }}
		'type' = CONFLUENT, 'access_region' = "{{.AccessRegion}}", 'uris' = '{{.Confluent.Uris.ValueString}}',
		'confluent.username' = '{{.Confluent.Username.ValueString}}', 'confluent.password' = '{{.Confluent.Password.ValueString}}'
	{{- end }}
	{{- if eq .Type "CONFLUENT_CLOUD" }}
		'type' = CONFLUENT_CLOUD, 'access_region' = "{{.AccessRegion}}", 'uris' = '{{.ConfluentCloud.Uris.ValueString}}',
		'confluent_cloud.key' = '{{.ConfluentCloud.Key.ValueString}}', 'confluent_cloud.secret' = '{{.ConfluentCloud.Secret.ValueString}}'
	{{- end }}
);`

// Create implements resource.Resource.
func (d *SchemaRegistryResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var sr SchemaRegistryResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &sr)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !sr.Owner.IsNull() && !sr.Owner.IsUnknown() {
		roleName = sr.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	var srtype string
	var confluentProperties ConfluentProperties
	var conflientCloudProperties ConfluentCloudProperties

	switch {
	case !sr.Confluent.IsNull() && !sr.Confluent.IsUnknown():
		srtype = "CONFLUENT"
		resp.Diagnostics.Append(sr.Confluent.As(ctx, &confluentProperties, basetypes.ObjectAsOptions{})...)
	case !sr.ConfluentCloud.IsNull() && !sr.ConfluentCloud.IsUnknown():
		srtype = "CONFLUENT_CLOUD"
		resp.Diagnostics.Append(sr.ConfluentCloud.As(ctx, &conflientCloudProperties, basetypes.ObjectAsOptions{})...)
	default:
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid schema registry", fmt.Errorf("must specify atleast one schema registry type properties"))
	}

	b := bytes.NewBuffer(nil)
	template.Must(template.New("").Parse(createStatement)).Execute(b, map[string]any{
		"Name":           sr.Name.ValueString(),
		"Type":           srtype,
		"AccessRegion":   sr.AccessRegion.ValueString(),
		"Confluent":      confluentProperties,
		"ConfluentCloud": conflientCloudProperties,
	})
	if _, err := conn.ExecContext(ctx, b.String()); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create schema registry", err)
		return
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		sr, err = d.updateComputed(ctx, conn, sr)
		if err != nil {
			return err
		}
		if sr.State.ValueString() != "ready" {
			return retry.RetryableError(fmt.Errorf("schema registry never transitioned to ready"))
		}
		return nil
	}); err != nil {
		if _, derr := conn.ExecContext(ctx, `DROP SCHEMA_REGISTRY "`+sr.Name.ValueString()+`";`); derr != nil {
			tflog.Error(ctx, "failed to clean up schema registry", map[string]any{
				"name":  sr.Name.ValueString(),
				"error": derr.Error(),
			})
		}

		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create schema registry", err)
		return
	}
	tflog.Info(ctx, "Schema registry created", map[string]any{"name": sr.Name.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, sr)...)
}

func (d *SchemaRegistryResource) updateComputed(ctx context.Context, conn *sql.Conn, sr SchemaRegistryResourceData) (SchemaRegistryResourceData, error) {
	rows, err := conn.QueryContext(ctx, `LIST SCHEMA_REGISTRIES;`)
	if err != nil {
		return sr, err
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var name string
		var srtype string
		var state string
		var owner string
		var updatedAt time.Time
		var createdAt time.Time
		if err := rows.Scan(&name, &srtype, &state, &discard, &owner, &createdAt, &updatedAt); err != nil {
			return sr, err
		}
		if name == sr.Name.ValueString() {
			sr.State = basetypes.NewStringValue(state)
			sr.Type = basetypes.NewStringValue(srtype)
			sr.Owner = basetypes.NewStringValue(owner)
			sr.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			sr.UpdatedAt = basetypes.NewStringValue(updatedAt.Format(time.RFC3339))
			return sr, nil
		}
	}
	return SchemaRegistryResourceData{}, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidSchemaRegistry}
}

func (d *SchemaRegistryResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var sr SchemaRegistryResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &sr)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !sr.Owner.IsNull() && !sr.Owner.IsUnknown() {
		roleName = sr.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA_REGISTRY "%s";`, sr.Name.ValueString())); err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidSchemaRegistry {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to drop schema registry", err)
			return
		}
	}
	tflog.Info(ctx, "Schema registry deleted", map[string]any{"name": sr.Name.ValueString()})
}

func (d *SchemaRegistryResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var sr SchemaRegistryResourceData
	var newSr SchemaRegistryResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newSr)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &sr)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	// all changes to secret other than ownership are disallowed
	if !newSr.Name.Equal(sr.Name) ||
		!newSr.Type.Equal(sr.Type) ||
		!newSr.AccessRegion.Equal(sr.AccessRegion) {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid update", fmt.Errorf("name, type and access region are immutable"))
	}

	if !newSr.Owner.IsNull() && !newSr.Owner.IsUnknown() && newSr.Owner.Equal(sr.Owner) {
		// Transfer ownership
		tflog.Error(ctx, "transfer ownership not yet supported")
	}

	sr, err = d.updateComputed(ctx, conn, sr)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, sr)...)
}

func (d *SchemaRegistryResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var Secret SchemaRegistryResourceData

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
		if errors.As(err, &godsErr) && godsErr.SQLCode == gods.SqlStateInvalidSchemaRegistry {
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, Secret)...)
}
