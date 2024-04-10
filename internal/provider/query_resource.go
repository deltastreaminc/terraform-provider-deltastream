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
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/sethvargo/go-retry"

	gods "github.com/deltastreaminc/go-deltastream"
)

var _ resource.Resource = &QueryResource{}
var _ resource.ResourceWithConfigure = &QueryResource{}

func NewQueryResource() resource.Resource {
	return &QueryResource{}
}

type QueryResource struct {
	cfg *DeltaStreamProviderCfg
}

type Query struct {
	Relations     basetypes.ListValue   `tfsdk:"relations"`
	Dsql          basetypes.StringValue `tfsdk:"dsql"`
	ID            basetypes.StringValue `tfsdk:"id"`
	Owner         basetypes.StringValue `tfsdk:"owner"`
	IntendedState basetypes.StringValue `tfsdk:"intended_state"`
	ActualState   basetypes.StringValue `tfsdk:"actual_state"`
	Message       basetypes.StringValue `tfsdk:"message"`
	CreatedAt     basetypes.StringValue `tfsdk:"created_at"`
	UpdatedAt     basetypes.StringValue `tfsdk:"updated_at"`
}

func (d *QueryResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Query resource",

		Attributes: map[string]schema.Attribute{
			"relations": schema.ListAttribute{
				Description: "Fully qualified relation names used in the query",
				Required:    true,
				ElementType: basetypes.StringType{},
			},
			"dsql": schema.StringAttribute{
				Description: "SQL query to be executed",
				Required:    true,
			},
			"id": schema.StringAttribute{
				Description: "ID of the query",
				Computed:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owner of the query",
				Optional:    true,
				Computed:    true,
			},
			"intended_state": schema.StringAttribute{
				Description: "Intended state of the query",
				Computed:    true,
			},
			"actual_state": schema.StringAttribute{
				Description: "State of the query",
				Computed:    true,
			},
			"message": schema.StringAttribute{
				Description: "Message from query execution",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the query",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Update date of the query",
				Computed:    true,
			},
		},
	}
}

func (d *QueryResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *QueryResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_query"
}

// Create implements resource.Resource.
func (d *QueryResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var q Query

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &q)...)
	if resp.Diagnostics.HasError() {
		return
	}

	role := d.cfg.Role
	if !q.Owner.IsNull() && !q.Owner.IsUnknown() {
		role = q.Owner.ValueString()
	}

	_, err := d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, role))
	if err != nil {
		resp.Diagnostics.AddError("failed to create Query", fmt.Sprintf(`failed to set role to "%s": %s`, role, err.Error()))
		return
	}

	row := d.cfg.Conn.QueryRowContext(ctx, q.Dsql.ValueString())
	var id string
	var discard any
	err = row.Scan(&discard, &id, &discard, &discard)
	if err != nil {
		resp.Diagnostics.AddError("failed to create Query", err.Error())
		return
	}
	tflog.Info(ctx, "Query launched", map[string]interface{}{"ID": id})
	q.ID = basetypes.NewStringValue(id)

	if err = d.waitForState(ctx, q, "running"); err != nil {
		resp.Diagnostics.AddError("failed to wait for running state", err.Error())
		return
	}

	q, err = d.updateComputed(ctx, q, false)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, q)...)
}

func (d *QueryResource) updateComputed(ctx context.Context, q Query, searchDeleted bool) (Query, error) {
	dsql := `LIST QUERIES`
	if searchDeleted {
		dsql += ` WITH ( 'all' )`
	}
	dsql += `;`

	rows, err := d.cfg.Conn.QueryContext(ctx, dsql)
	if err != nil {
		return q, err
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var id string
		var intendedState string
		var actualState string
		var owner string
		var createdAt time.Time
		var updatedAt time.Time
		err := rows.Scan(&id, &intendedState, &actualState, &discard, &owner, &createdAt, &updatedAt)
		if err != nil {
			return q, err
		}
		if id == q.ID.ValueString() {
			q.IntendedState = basetypes.NewStringValue(intendedState)
			q.ActualState = basetypes.NewStringValue(actualState)
			q.Owner = basetypes.NewStringValue(owner)
			q.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			q.UpdatedAt = basetypes.NewStringValue(updatedAt.Format(time.RFC3339))
			q.Message = basetypes.NewStringValue("")
			return q, nil
		}
	}

	return Query{}, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidQuery}
}

func (d *QueryResource) waitForState(ctx context.Context, q Query, expectedState string) error {
	return retry.Do(ctx, retry.WithMaxDuration(5*time.Minute, retry.NewConstant(time.Second*5)), func(ctx context.Context) error {
		q, err := d.updateComputed(ctx, q, true)
		if err != nil {
			return err
		}

		if q.ActualState.ValueString() == expectedState {
			return nil
		}
		return retry.RetryableError(fmt.Errorf("Query state is %s, expected %s", q.ActualState.ValueString(), expectedState))
	})
}

func (d *QueryResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var Query Query

	resp.Diagnostics.Append(req.State.Get(ctx, &Query)...)
	if resp.Diagnostics.HasError() {
		return
	}

	_, err := d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, d.cfg.Role))
	if err != nil {
		resp.Diagnostics.AddError("Failed to set role", err.Error())
		return
	}

	_, err = d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`TERMINATE QUERY %s;`, Query.ID.ValueString()))
	if err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidQuery {
			resp.Diagnostics.AddError("Failed to drop Query", err.Error())
			return
		}
	}

	if err = d.waitForState(ctx, Query, "stopped"); err != nil {
		var dserr gods.ErrSQLError
		if errors.As(err, &dserr) && dserr.SQLCode == gods.SqlStateInvalidQuery {
			return
		}
		resp.Diagnostics.AddError("failed to wait for deletion", err.Error())
		return
	}
}

func (d *QueryResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var currentQuery Query
	var newQuery Query

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newQuery)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &currentQuery)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if newQuery.Dsql != currentQuery.Dsql {
		resp.Diagnostics.AddError("Query SQL cannot be changed", "Query SQL is immutable")
		return
	}

	// if newDB.Owner != nil && *newDB.Owner != *currentDB.Owner {
	// 	// transfer ownership
	// }

	currentQuery, err := d.updateComputed(ctx, currentQuery, false)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, currentQuery)...)
}

func (d *QueryResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var Query Query

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &Query)...)
	if resp.Diagnostics.HasError() {
		return
	}

	Query, err := d.updateComputed(ctx, Query, true)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, Query)...)
}
