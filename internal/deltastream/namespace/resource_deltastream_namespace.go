// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package namespace

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

var _ resource.Resource = &NamespaceResource{}
var _ resource.ResourceWithConfigure = &NamespaceResource{}

func NewNamespaceResource() resource.Resource {
	return &NamespaceResource{}
}

type NamespaceResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type NamespaceResourceData struct {
	Database  types.String `tfsdk:"database"`
	Name      types.String `tfsdk:"name"`
	Owner     types.String `tfsdk:"owner"`
	CreatedAt types.String `tfsdk:"created_at"`
}

func (d *NamespaceResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Namespace resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
			},
			"name": schema.StringAttribute{
				Description: "Name of the Namespace",
				Required:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Namespace",
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

func (d *NamespaceResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *NamespaceResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_namespace"
}

// Create implements resource.Resource.
func (d *NamespaceResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var namespace NamespaceResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &namespace)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !namespace.Owner.IsNull() && !namespace.Owner.IsUnknown() {
		roleName = namespace.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(createNamespaceTmpl, map[string]any{
		"DatabaseName": namespace.Database.ValueString(),
		"Name":         namespace.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	if _, err := conn.ExecContext(ctx, dsql); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create namespace", err)
		return
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		namespace, err = d.updateComputed(ctx, conn, namespace)
		if err != nil {
			var sqlErr gods.ErrSQLError
			if errors.As(err, &sqlErr) && sqlErr.SQLCode == gods.SqlStateInvalidSchema {
				return err
			}
			return retry.RetryableError(err)
		}
		return nil
	}); err != nil {
		dsql, derr := util.ExecTemplate(dropNamespaceTmpl, map[string]any{
			"DatabaseName": namespace.Database.ValueString(),
			"Name":         namespace.Name.ValueString(),
		})
		if derr != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", derr)
			return
		}
		if _, derr := conn.ExecContext(ctx, dsql); derr != nil {
			tflog.Error(ctx, "failed to clean up namespace", map[string]any{
				"name":  namespace.Name.ValueString(),
				"error": derr.Error(),
			})
		}

		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create namespace", err)
		return
	}
	tflog.Info(ctx, "Namespace created", map[string]any{"name": namespace.Name.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, namespace)...)
}

func (d *NamespaceResource) updateComputed(ctx context.Context, conn *sql.Conn, namespace NamespaceResourceData) (NamespaceResourceData, error) {
	dsql, err := util.ExecTemplate(lookupNamespaceTmpl, map[string]any{
		"DatabaseName": namespace.Database.ValueString(),
		"Name":         namespace.Name.ValueString(),
	})
	if err != nil {
		return namespace, fmt.Errorf("failed to generate SQL: %w", err)
	}
	row := conn.QueryRowContext(ctx, dsql)
	if err := row.Err(); err != nil {
		return namespace, fmt.Errorf("failed to lookup namespace: %w", err)
	}

	var owner string
	var createdAt time.Time
	if err := row.Scan(&owner, &createdAt); err != nil {
		if err == sql.ErrNoRows {
			return namespace, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidSchema}
		}
		return namespace, fmt.Errorf("failed to read namespace: %w", err)
	}

	namespace.Owner = types.StringValue(owner)
	namespace.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	return namespace, nil
}

func (d *NamespaceResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var namespace NamespaceResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &namespace)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !namespace.Owner.IsNull() && !namespace.Owner.IsUnknown() {
		roleName = namespace.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(dropNamespaceTmpl, map[string]any{
		"DatabaseName": namespace.Database.ValueString(),
		"Name":         namespace.Name.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	if _, err := conn.ExecContext(ctx, dsql); err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || (sqlErr.SQLCode != gods.SqlStateInvalidDatabase && sqlErr.SQLCode != gods.SqlStateInvalidSchema) {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to delete namespace", err)
			return
		}
	}
	tflog.Info(ctx, "Namespace deleted", map[string]any{"name": namespace.Name.ValueString()})
}

func (d *NamespaceResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "update not supported", fmt.Errorf("namespace updates not supported"))
}

func (d *NamespaceResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var namespace NamespaceResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &namespace)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !namespace.Owner.IsNull() && !namespace.Owner.IsUnknown() {
		roleName = namespace.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	namespace, err = d.updateComputed(ctx, conn, namespace)
	if err != nil {
		var sqlErr gods.ErrSQLError
		if errors.As(err, &sqlErr) && sqlErr.SQLCode == gods.SqlStateInvalidSchema {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, namespace)...)
}
