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
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/sethvargo/go-retry"

	gods "github.com/deltastreaminc/go-deltastream"
)

var _ resource.Resource = &RelationResource{}
var _ resource.ResourceWithConfigure = &RelationResource{}

func NewRelationResource() resource.Resource {
	return &RelationResource{}
}

type RelationResource struct {
	cfg *DeltaStreamProviderCfg
}

type RelationProperties struct {
	Name basetypes.StringValue `tfsdk:"name"`
}

type Relation struct {
	Name         basetypes.StringValue `tfsdk:"name"`
	Database     basetypes.StringValue `tfsdk:"database"`
	Schema       basetypes.StringValue `tfsdk:"schema"`
	Store        basetypes.StringValue `tfsdk:"store"`
	Dsql         basetypes.StringValue `tfsdk:"dsql"`
	RelationType basetypes.StringValue `tfsdk:"type"`
	State        basetypes.StringValue `tfsdk:"state"`
	Owner        basetypes.StringValue `tfsdk:"owner"`
	UpdatedAt    basetypes.StringValue `tfsdk:"updated_at"`
	CreatedAt    basetypes.StringValue `tfsdk:"created_at"`
}

func (d *RelationResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Relation resource",

		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the Relation",
				Required:    true,
			},
			"database": schema.StringAttribute{
				Description: "Name of the database containing the relation",
				Required:    true,
			},
			"schema": schema.StringAttribute{
				Description: "Name of the database schema containing the relation",
				Required:    true,
			},
			"store": schema.StringAttribute{
				Description: "Name of the store backing the relation topic",
				Required:    true,
			},
			"dsql": schema.StringAttribute{
				Description: "relation definition in DSQL format",
				Required:    true,
			},
			"type": schema.StringAttribute{
				Description: "Relation type",
				Computed:    true,
			},
			"state": schema.StringAttribute{
				Description: "State of the Relation",
				Computed:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Relation",
				Optional:    true,
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Last updated date of the Relation",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the Relation",
				Computed:    true,
			},
		},
	}
}

func (d *RelationResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *RelationResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_relation"
}

// Create implements resource.Resource.
func (d *RelationResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var r Relation

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &r)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := d.setEnv(ctx, r)
	if err != nil {
		resp.Diagnostics.AddError("failed to create relation", err.Error())
		return
	}

	_, err = d.cfg.Conn.ExecContext(ctx, r.Dsql.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("failed to create relation", err.Error())
		return
	}

	if err = d.waitForState(ctx, r, "created"); err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	r, err = d.updateComputed(ctx, r)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, r)...)
}

func (d *RelationResource) setEnv(ctx context.Context, r Relation) error {
	role := d.cfg.Role
	if !r.Owner.IsNull() && !r.Owner.IsUnknown() {
		role = r.Owner.ValueString()
	}
	_, err := d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, role))
	if err != nil {
		return fmt.Errorf(`failed to set role to "%s": %s`, role, err.Error())
	}

	_, err = d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE DATABASE "%s";`, r.Database.ValueString()))
	if err != nil {
		return fmt.Errorf(`failed to use database "%s": %s`, r.Database.ValueString(), err.Error())
	}

	// fixme
	// _, err = d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE SCHEMA "%s";`, r.Schema.ValueString()))
	// if err != nil {
	// 	return fmt.Errorf(`failed to use schema "%s": %s`, r.Schema.ValueString(), err.Error())
	// }

	_, err = d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE STORE "%s";`, r.Store.ValueString()))
	if err != nil {
		return fmt.Errorf(`failed to use store "%s": %s`, r.Store.ValueString(), err.Error())
	}
	return nil
}

func (d *RelationResource) updateComputed(ctx context.Context, r Relation) (Relation, error) {
	rows, err := d.cfg.Conn.QueryContext(ctx, `LIST RelationS;`, r.Name.ValueString())
	if err != nil {
		return r, err
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var name string
		var rtype string
		var state string
		var owner string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &rtype, &owner, &state, &discard, &createdAt, &updatedAt); err != nil {
			return r, err
		}
		if name == r.Name.ValueString() {
			r.RelationType = basetypes.NewStringValue(rtype)
			r.State = basetypes.NewStringValue(state)
			r.Owner = basetypes.NewStringValue(owner)
			r.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			r.UpdatedAt = basetypes.NewStringValue(updatedAt.Format(time.RFC3339))
			return r, nil
		}
	}
	return Relation{}, gods.ErrSQLError{SQLCode: gods.SqlStateInvalidRelation}
}

func (d *RelationResource) waitForState(ctx context.Context, Relation Relation, expectedState string) error {
	return retry.Do(ctx, retry.WithMaxDuration(5*time.Minute, retry.NewConstant(time.Second*5)), func(ctx context.Context) error {
		r, err := d.updateComputed(ctx, Relation)
		if err != nil {
			return err
		}
		if r.State.ValueString() == expectedState {
			return nil
		}
		return retry.RetryableError(fmt.Errorf("Relation state is %s, expected %s", r.State.ValueString(), expectedState))
	})
}

func (d *RelationResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var r Relation

	resp.Diagnostics.Append(req.State.Get(ctx, &r)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := d.setEnv(ctx, r)
	if err != nil {
		resp.Diagnostics.AddError("failed to drop relation", err.Error())
		return
	}

	_, err = d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`DROP Relation "%s";`, r.Name.ValueString()))
	if err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidRelation {
			resp.Diagnostics.AddError("failed to drop relation", err.Error())
			return
		}
	}

	if err = d.waitForState(ctx, r, "deleted"); err != nil {
		var dserr gods.ErrSQLError
		if errors.As(err, &dserr) && dserr.SQLCode == gods.SqlStateInvalidRelation {
			return
		}
		resp.Diagnostics.AddError("failed to wait for deletion", err.Error())
		return
	}
}

func (d *RelationResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics.AddError("unable to update relation", "relations are immutable")
	return
}

func (d *RelationResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var Relation Relation

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &Relation)...)
	if resp.Diagnostics.HasError() {
		return
	}

	Relation, err := d.updateComputed(ctx, Relation)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, Relation)...)
}
