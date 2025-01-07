// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

var _ resource.Resource = &SchemaResource{}
var _ resource.ResourceWithConfigure = &SchemaResource{}

func NewSchemaResource() resource.Resource {
	return &SchemaResource{}
}

type SchemaResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type SchemaResourceData struct {
	Database  types.String `tfsdk:"database"`
	Name      types.String `tfsdk:"name"`
	Owner     types.String `tfsdk:"owner"`
	CreatedAt types.String `tfsdk:"created_at"`
}

func (d *SchemaResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Schema resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
			},
			"name": schema.StringAttribute{
				Description: "Name of the Schema",
				Required:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the schema",
				Optional:    true,
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the schema",
				Computed:    true,
			},
		},
	}
}

func (d *SchemaResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *SchemaResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schema"
}

// Create implements resource.Resource.
func (d *SchemaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var schema SchemaResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &schema)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !schema.Owner.IsNull() && !schema.Owner.IsUnknown() {
		roleName = schema.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(createSchemaTmpl, map[string]any{
		"DatabaseName": schema.Database.ValueString(),
		"Name":         schema.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	if _, err := conn.ExecContext(ctx, dsql); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create schema", err)
		return
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		schema, err = d.updateComputed(ctx, conn, schema)
		if err != nil {
			var sqlErr gods.ErrSQLError
			if errors.As(err, &sqlErr) && sqlErr.SQLCode == gods.SqlStateInvalidSchema {
				return err
			}
			return retry.RetryableError(err)
		}
		return nil
	}); err != nil {
		dsql, derr := util.ExecTemplate(dropSchemaTmpl, map[string]any{
			"DatabaseName": schema.Database.ValueString(),
			"Name":         schema.Name.ValueString(),
		})
		if derr != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", derr)
			return
		}
		if _, derr := conn.ExecContext(ctx, dsql); derr != nil {
			tflog.Error(ctx, "failed to clean up schema", map[string]any{
				"name":  schema.Name.ValueString(),
				"error": derr.Error(),
			})
		}

		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create schema", err)
		return
	}
	tflog.Info(ctx, "Schema created", map[string]any{"name": schema.Name.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, schema)...)
}

func (d *SchemaResource) updateComputed(ctx context.Context, conn *sql.Conn, schema SchemaResourceData) (SchemaResourceData, error) {
	dsql, err := util.ExecTemplate(lookupSchemaTmpl, map[string]any{
		"DatabaseName": schema.Database.ValueString(),
		"Name":         schema.Name.ValueString(),
	})
	if err != nil {
		return schema, fmt.Errorf("failed to generate SQL: %w", err)
	}
	row := conn.QueryRowContext(ctx, dsql)
	if err := row.Err(); err != nil {
		return schema, fmt.Errorf("failed to lookup schema: %w", err)
	}

	var owner string
	var createdAt time.Time
	if err := row.Scan(&owner, &createdAt); err != nil {
		if err == sql.ErrNoRows {
			return schema, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidSchema}
		}
		return schema, fmt.Errorf("failed to read schema: %w", err)
	}

	schema.Owner = types.StringValue(owner)
	schema.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	return schema, nil
}

func (d *SchemaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var schema SchemaResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &schema)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !schema.Owner.IsNull() && !schema.Owner.IsUnknown() {
		roleName = schema.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(dropSchemaTmpl, map[string]any{
		"DatabaseName": schema.Database.ValueString(),
		"Name":         schema.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	if _, err := conn.ExecContext(ctx, dsql); err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || (sqlErr.SQLCode != gods.SqlStateInvalidDatabase && sqlErr.SQLCode != gods.SqlStateInvalidSchema) {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to delete schema", err)
			return
		}
	}
	tflog.Info(ctx, "Schema deleted", map[string]any{"name": schema.Name.ValueString()})
}

func (d *SchemaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "update not supported", fmt.Errorf("schema updates not supported"))
}

func (d *SchemaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var schema SchemaResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &schema)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !schema.Owner.IsNull() && !schema.Owner.IsUnknown() {
		roleName = schema.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	schema, err = d.updateComputed(ctx, conn, schema)
	if err != nil {
		var sqlErr gods.ErrSQLError
		if errors.As(err, &sqlErr) && sqlErr.SQLCode == gods.SqlStateInvalidSchema {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, schema)...)
}
