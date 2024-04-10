// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/sethvargo/go-retry"

	gods "github.com/deltastreaminc/go-deltastream"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
)

var _ resource.Resource = &StoreResource{}
var _ resource.ResourceWithConfigure = &StoreResource{}

func NewStoreResource() resource.Resource {
	return &StoreResource{}
}

type StoreResource struct {
	cfg *DeltaStreamProviderCfg
}

type StoreProperties struct {
	Uris          basetypes.StringValue `tfsdk:"uris"`
	AccessRegion  basetypes.StringValue `tfsdk:"access_region"`
	SaslHashFunc  basetypes.StringValue `tfsdk:"kafka_sasl_hash_function"`
	SaslUsername  basetypes.StringValue `tfsdk:"kafka_sasl_username"`
	SaslPassword  basetypes.StringValue `tfsdk:"kafka_sasl_password"`
	MskIamRoleArn basetypes.StringValue `tfsdk:"kafka_msk_iam_role_arn"`
	MskAwsRegion  basetypes.StringValue `tfsdk:"kafka_msk_aws_region"`
	TlsDisabled   basetypes.BoolValue   `tfsdk:"tls_disabled"`
}

type Store struct {
	Name            basetypes.StringValue `tfsdk:"name"`
	IsDefault       basetypes.BoolValue   `tfsdk:"is_default"`
	StoreType       basetypes.StringValue `tfsdk:"type"`
	StoreProperties basetypes.ObjectValue `tfsdk:"properties"`
	Owner           basetypes.StringValue `tfsdk:"owner"`
	State           basetypes.StringValue `tfsdk:"state"`
	UpdatedAt       basetypes.StringValue `tfsdk:"updated_at"`
	CreatedAt       basetypes.StringValue `tfsdk:"created_at"`
}

func (d *StoreResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Store resource",

		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the Store",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"type": schema.StringAttribute{
				Description: "Store type. Supported types are: KAFKA",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"properties": schema.SingleNestedAttribute{
				Description: "Store properties",
				Required:    true,
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "URIs of the Store",
						Required:    true,
						Validators:  []validator.String{util.UrlsValidator{}},
					},
					"access_region": schema.StringAttribute{
						Description: "Access region of the Store",
						Required:    true,
					},
					"kafka_sasl_hash_function": schema.StringAttribute{
						Description: "SASL  hash function",
						Optional:    true,
					},
					"kafka_sasl_username": schema.StringAttribute{
						Description: "SASL username",
						Optional:    true,
					},
					"kafka_sasl_password": schema.StringAttribute{
						Description: "SASL password",
						Optional:    true,
					},
					"kafka_msk_iam_role_arn": schema.StringAttribute{
						Description: "MSK IAM role ARN",
						Optional:    true,
					},
					"kafka_msk_aws_region": schema.StringAttribute{
						Description: "MSK AWS region",
						Optional:    true,
						Validators:  util.IdentifierValidators,
					},
					"tls_disabled": schema.BoolAttribute{
						Description: "Disable TLS",
						Optional:    true,
					},
				},
			},
			"is_default": schema.BoolAttribute{
				Description: "Is the Store the default",
				Computed:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Store",
				Optional:    true,
				Computed:    true,
				Validators:  util.IdentifierValidators,
			},
			"state": schema.StringAttribute{
				Description: "State of the Store",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Last updated date of the Store",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the Store",
				Computed:    true,
			},
		},
	}
}

func (d *StoreResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}

	cfg, ok := req.ProviderData.(*DeltaStreamProviderCfg)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *DeltaStreamProviderCfg, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)
		return
	}

	d.cfg = cfg
}

func (d *StoreResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_store"
}

// Create implements resource.Resource.
func (d *StoreResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var store Store

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	role := d.cfg.Role
	if !store.Owner.IsNull() && !store.Owner.IsUnknown() {
		role = store.Owner.ValueString()
	}
	_, err := d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, role))
	if err != nil {
		resp.Diagnostics.AddError("failed to create store", fmt.Sprintf(`failed to set role to "%s": %s`, role, err.Error()))
		return
	}

	var props StoreProperties
	resp.Diagnostics.Append(store.StoreProperties.As(ctx, &props, basetypes.ObjectAsOptions{})...)
	if resp.Diagnostics.HasError() {
		return
	}

	dsql := fmt.Sprintf(
		`CREATE STORE IF NOT EXISTS "%s" WITH('type'=%s, 'uris'='%s', 'access_region'="%s"`,
		store.Name.ValueString(), store.StoreType.ValueString(), props.Uris.ValueString(), props.AccessRegion.ValueString(),
	)
	if !props.SaslHashFunc.IsNull() && !props.SaslHashFunc.IsUnknown() {
		dsql += fmt.Sprintf(`, 'kafka.sasl.hash_function'='%s'`, props.SaslHashFunc.ValueString())
	}
	if !props.SaslUsername.IsNull() && !props.SaslUsername.IsUnknown() {
		dsql += fmt.Sprintf(`, 'kafka.sasl.username'='%s'`, props.SaslUsername.ValueString())
	}
	if !props.SaslPassword.IsNull() && !props.SaslPassword.IsUnknown() {
		dsql += fmt.Sprintf(`, 'kafka.sasl.password'='%s'`, props.SaslPassword.ValueString())
	}
	if !props.MskIamRoleArn.IsNull() && !props.MskIamRoleArn.IsUnknown() {
		dsql += fmt.Sprintf(`, 'kafka.msk.iam_role_arn'='%s'`, props.MskIamRoleArn.ValueString())
	}
	if !props.MskAwsRegion.IsNull() && !props.MskAwsRegion.IsUnknown() {
		dsql += fmt.Sprintf(`, 'kafka.msk.aws_region'='%s'`, props.MskAwsRegion.ValueString())
	}
	if !props.TlsDisabled.IsNull() && !props.TlsDisabled.IsUnknown() {
		dsql += fmt.Sprintf(`, 'tls.disabled'=%t`, props.TlsDisabled.ValueBool())
	}
	dsql += `);`

	_, err = d.cfg.Conn.ExecContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics.AddError("failed to create store", err.Error())
		return
	}

	if err = d.waitForState(ctx, store, "ready"); err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	store, err = d.updateComputed(ctx, store)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, store)...)
}

func (d *StoreResource) updateComputed(ctx context.Context, store Store) (Store, error) {
	rows, err := d.cfg.Conn.QueryContext(ctx, `LIST STORES;`, store.Name.ValueString())
	if err != nil {
		return store, err
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var name string
		var state string
		var isDefault bool
		var owner string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &discard, &discard, &state, &isDefault, &owner, &createdAt, &updatedAt); err != nil {
			return store, err
		}
		if name == store.Name.ValueString() {
			store.IsDefault = basetypes.NewBoolValue(isDefault)
			store.Owner = basetypes.NewStringValue(owner)
			store.State = basetypes.NewStringValue(state)
			store.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			store.UpdatedAt = basetypes.NewStringValue(updatedAt.Format(time.RFC3339))
			return store, nil
		}
	}
	return Store{}, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidStore}
}

func (d *StoreResource) waitForState(ctx context.Context, store Store, expectedState string) error {
	return retry.Do(ctx, retry.WithMaxDuration(5*time.Minute, retry.NewConstant(time.Second*5)), func(ctx context.Context) error {
		row := d.cfg.Conn.QueryRowContext(ctx, fmt.Sprintf(`DESCRIBE STORE "%s";`, store.Name.ValueString()))

		var state string
		var discard any
		if err := row.Scan(&discard, &discard, &discard, &state, &discard, &discard, &discard, &discard, &discard, &discard, &discard, &discard); err != nil {
			return err
		}
		if state == expectedState {
			return nil
		}
		return retry.RetryableError(fmt.Errorf("store state is %s, expected %s", state, expectedState))
	})
}

func (d *StoreResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var store Store

	resp.Diagnostics.Append(req.State.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	_, err := d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, d.cfg.Role))
	if err != nil {
		resp.Diagnostics.AddError("Failed to set role", err.Error())
		return
	}

	_, err = d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`DROP STORE "%s";`, store.Name.ValueString()))
	if err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidStore {
			resp.Diagnostics.AddError("Failed to drop store", err.Error())
			return
		}
	}

	if err = d.waitForState(ctx, store, "deleted"); err != nil {
		var dserr gods.ErrSQLError
		if errors.As(err, &dserr) && dserr.SQLCode == gods.SqlStateInvalidStore {
			return
		}
		resp.Diagnostics.AddError("failed to wait for deletion", err.Error())
		return
	}
}

func (d *StoreResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var currentStore Store
	var newStore Store

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newStore)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &currentStore)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if newStore.Name != currentStore.Name {
		resp.Diagnostics.AddError("store name cannot be changed", "store name is immutable")
		return
	}

	// if newDB.Owner != nil && *newDB.Owner != *currentDB.Owner {
	// 	// transfer ownership
	// }

	currentStore, err := d.updateComputed(ctx, currentStore)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, currentStore)...)
}

func (d *StoreResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var store Store

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	store, err := d.updateComputed(ctx, store)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, store)...)
}
