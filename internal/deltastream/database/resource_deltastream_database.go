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
	"github.com/hashicorp/terraform-plugin-framework/types"
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
	Name      types.String `tfsdk:"name"`
	Owner     types.String `tfsdk:"owner"`
	CreatedAt types.String `tfsdk:"created_at"`
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
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "internal error", fmt.Errorf("invalid provider data"))
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

	roleName := d.cfg.Role
	if !database.Owner.IsNull() && !database.Owner.IsUnknown() {
		roleName = database.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	b := bytes.NewBuffer(nil)
	template.Must(template.New("").Parse(createStatement)).Execute(b, map[string]any{
		"Name": database.Name.ValueString(),
	})
	if _, err := conn.ExecContext(ctx, b.String()); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create database", err)
		return
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		database, err = d.updateComputed(ctx, conn, database)
		if err != nil {
			var godsErr gods.ErrSQLError
			if errors.As(err, &godsErr) && godsErr.SQLCode == gods.SqlStateInvalidDatabase {
				return err
			}
			return retry.RetryableError(err)
		}
		return nil
	}); err != nil {
		if _, derr := conn.ExecContext(ctx, `DROP DATABASE "`+database.Name.ValueString()+`";`); derr != nil {
			tflog.Error(ctx, "failed to clean up database", map[string]any{
				"name":  database.Name.ValueString(),
				"error": derr.Error(),
			})
		}

		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create database", err)
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
			db.Owner = types.StringValue(owner)
			db.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
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

	roleName := d.cfg.Role
	if !database.Owner.IsNull() && !database.Owner.IsUnknown() {
		roleName = database.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if err = retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE "%s";`, database.Name.ValueString())); err != nil {
			var sqlErr gods.ErrSQLError
			if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidDatabase {
				return retry.RetryableError(err)
			}
		}
		return nil
	}); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to delete database", err)
		return
	}
	tflog.Info(ctx, "Database deleted", map[string]any{"name": database.Name.ValueString()})
}

func (d *DatabaseResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "update not supported", fmt.Errorf("database updates not supported"))
}

func (d *DatabaseResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var database DatabaseResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &database)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !database.Owner.IsNull() && !database.Owner.IsUnknown() {
		roleName = database.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	database, err = d.updateComputed(ctx, conn, database)
	if err != nil {
		var godsErr gods.ErrSQLError
		if errors.As(err, &godsErr) && godsErr.SQLCode == gods.SqlStateInvalidDatabase {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to read database state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, database)...)
}
