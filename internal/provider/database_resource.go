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

	gods "github.com/deltastreaminc/go-deltastream"
)

var _ resource.Resource = &DatabaseResource{}
var _ resource.ResourceWithConfigure = &DatabaseResource{}

func NewDatabaseResource() resource.Resource {
	return &DatabaseResource{}
}

type DatabaseResource struct {
	cfg *DeltaStreamProviderCfg
}

type Database struct {
	Name      basetypes.StringValue `tfsdk:"name"`
	IsDefault basetypes.BoolValue   `tfsdk:"is_default"`
	Owner     basetypes.StringValue `tfsdk:"owner"`
	CreatedAt basetypes.StringValue `tfsdk:"created_at"`
}

func (d *DatabaseResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Database resource",

		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
			},
			"is_default": schema.BoolAttribute{
				Description: "Is the Database the default",
				Computed:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Database",
				Optional:    true,
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the Database",
				Computed:    true,
			},
		},
	}
}

func (d *DatabaseResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *DatabaseResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
}

// Create implements resource.Resource.
func (d *DatabaseResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var db Database

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &db)...)
	if resp.Diagnostics.HasError() {
		return
	}

	role := d.cfg.Role
	if !db.Owner.IsNull() && !db.Owner.IsUnknown() {
		role = db.Owner.ValueString()
	}
	_, err := d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, role))
	if err != nil {
		resp.Diagnostics.AddError("failed to create database", fmt.Sprintf(`failed to set role to "%s": %s`, role, err.Error()))
		return
	}

	dsql := fmt.Sprintf(`CREATE Database IF NOT EXISTS "%s";`, db.Name.ValueString())
	_, err = d.cfg.Conn.ExecContext(ctx, dsql)
	if err != nil {
		resp.Diagnostics.AddError("failed to create database", err.Error())
		return
	}

	db, err = d.updateComputed(ctx, db)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, db)...)
}

func (d *DatabaseResource) updateComputed(ctx context.Context, db Database) (Database, error) {
	rows, err := d.cfg.Conn.QueryContext(ctx, `LIST DATABASES;`)
	if err != nil {
		return db, err
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var isDefault bool
		var owner string
		var createdAt time.Time
		if err := rows.Scan(&name, &isDefault, &owner, &createdAt); err != nil {
			return db, err
		}
		if name == db.Name.ValueString() {
			db.IsDefault = basetypes.NewBoolValue(isDefault)
			db.Owner = basetypes.NewStringValue(owner)
			db.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			return db, nil
		}
	}
	return Database{}, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidDatabase}
}

func (d *DatabaseResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var Database Database

	resp.Diagnostics.Append(req.State.Get(ctx, &Database)...)
	if resp.Diagnostics.HasError() {
		return
	}

	_, err := d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, d.cfg.Role))
	if err != nil {
		resp.Diagnostics.AddError("Failed to set role", err.Error())
		return
	}

	_, err = d.cfg.Conn.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE "%s";`, Database.Name.ValueString()))
	if err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidDatabase {
			resp.Diagnostics.AddError("Failed to drop Database", err.Error())
			return
		}
	}
}

func (d *DatabaseResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var currentDB Database
	var newDB Database

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newDB)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &currentDB)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if newDB.Name != currentDB.Name {
		resp.Diagnostics.AddError("Database name cannot be changed", "Database name is immutable")
		return
	}

	// if newDB.Owner != nil && *newDB.Owner != *currentDB.Owner {
	// 	// transfer ownership
	// }

	currentDB, err := d.updateComputed(ctx, currentDB)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, currentDB)...)
}

func (d *DatabaseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var Database Database

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &Database)...)
	if resp.Diagnostics.HasError() {
		return
	}

	Database, err := d.updateComputed(ctx, Database)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, Database)...)
}
