// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package database

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

var _ resource.Resource = &DatabaseResource{}
var _ resource.ResourceWithConfigure = &DatabaseResource{}

func NewDatabaseResource() resource.Resource {
	return &DatabaseResource{}
}

type DatabaseResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type DatabaseResourceData struct {
	Name      basetypes.StringValue `tfsdk:"name"`
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
				Validators:  util.IdentifierValidators,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Database",
				Optional:    true,
				Computed:    true,
				Validators:  util.IdentifierValidators,
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

	cfg, ok := req.ProviderData.(*config.DeltaStreamProviderCfg)
	if !ok {
		resp.Diagnostics.AddError("internal error", "invalid provider data")
		return
	}

	d.cfg = cfg
}

func (d *DatabaseResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_database"
}

const createStatement = `CREATE DATABASE "{{.Name}}";`

// Create implements resource.Resource.
func (d *DatabaseResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var database DatabaseResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &database)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect to database", err.Error())
		return
	}
	defer conn.Close()

	roleName := d.cfg.Role
	if !database.Owner.IsNull() && !database.Owner.IsUnknown() {
		roleName = database.Owner.ValueString()
	}

	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		resp.Diagnostics.AddError("failed to set sql context", err.Error())
		return
	}

	b := bytes.NewBuffer(nil)
	template.Must(template.New("").Parse(createStatement)).Execute(b, map[string]any{
		"Name": database.Name.ValueString(),
	})
	if _, err := conn.ExecContext(ctx, b.String()); err != nil {
		resp.Diagnostics.AddError("failed to create database", err.Error())
		return
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		database, err = d.updateComputed(ctx, conn, database)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		if _, derr := conn.ExecContext(ctx, `DROP DATABASE "`+database.Name.ValueString()+`";`); derr != nil {
			tflog.Error(ctx, "failed to clean up database", map[string]any{
				"name":  database.Name.ValueString(),
				"error": derr.Error(),
			})
		}

		resp.Diagnostics.AddError("failed to create database", err.Error())
		return
	}
	tflog.Info(ctx, "Database created", map[string]any{"name": database.Name.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, database)...)
}

func (d *DatabaseResource) updateComputed(ctx context.Context, conn *sql.Conn, db DatabaseResourceData) (DatabaseResourceData, error) {
	rows, err := conn.QueryContext(ctx, `LIST DATABASES;`)
	if err != nil {
		return db, err
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var name string
		var owner string
		var createdAt time.Time
		if err := rows.Scan(&name, &discard, &owner, &createdAt); err != nil {
			return db, err
		}
		if name == db.Name.ValueString() {
			db.Owner = basetypes.NewStringValue(owner)
			db.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			return db, nil
		}
	}
	return DatabaseResourceData{}, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidDatabase}
}

func (d *DatabaseResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var database DatabaseResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &database)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect to database", err.Error())
		return
	}
	defer conn.Close()

	roleName := d.cfg.Role
	if !database.Owner.IsNull() && !database.Owner.IsUnknown() {
		roleName = database.Owner.ValueString()
	}
	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		resp.Diagnostics.AddError("failed to set sql context", err.Error())
		return
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE "%s";`, database.Name.ValueString())); err != nil {
			var sqlErr gods.ErrSQLError
			if !errors.As(err, &sqlErr) {
				return err
			}
			if sqlErr.SQLCode == gods.SqlStateInvalidDatabase {
				return nil
			}
			if sqlErr.SQLCode == gods.SqlStateDependentObjectsStillExist {
				return retry.RetryableError(err)
			}
		}
		return nil
	}); err != nil {
		resp.Diagnostics.AddError("failed to delete database", err.Error())
		return
	}
	tflog.Info(ctx, "Database deleted", map[string]any{"name": database.Name.ValueString()})
}

func (d *DatabaseResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var currentDatabase DatabaseResourceData
	var newDatabase DatabaseResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newDatabase)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &currentDatabase)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect to database", err.Error())
		return
	}
	defer conn.Close()

	// all changes to database other than ownership are disallowed
	if !newDatabase.Name.Equal(currentDatabase.Name) {
		resp.Diagnostics.AddError("invalid update", "database name cannot be changed")
	}

	if !newDatabase.Owner.IsNull() && !newDatabase.Owner.IsUnknown() && newDatabase.Owner.Equal(currentDatabase.Owner) {
		// Transfer ownership
		tflog.Error(ctx, "transfer ownership not yet supported")
	}

	currentDatabase, err = d.updateComputed(ctx, conn, currentDatabase)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, currentDatabase)...)
}

func (d *DatabaseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var database DatabaseResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &database)...)
	if resp.Diagnostics.HasError() {
		return
	}

	conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect to database", err.Error())
		return
	}
	defer conn.Close()

	database, err = d.updateComputed(ctx, conn, database)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, database)...)
}
