// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package object

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/sethvargo/go-retry"

	gods "github.com/deltastreaminc/go-deltastream"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
)

var _ resource.Resource = &ObjectResource{}
var _ resource.ResourceWithConfigure = &ObjectResource{}

func NewObjectResource() resource.Resource {
	return &ObjectResource{}
}

type ObjectResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type ObjectResourceData struct {
	Database  types.String `tfsdk:"database"`
	Namespace types.String `tfsdk:"namespace"`
	Name      types.String `tfsdk:"name"`
	Store     types.String `tfsdk:"store"`
	Sql       types.String `tfsdk:"sql"`

	FQN       types.String `tfsdk:"fqn"`
	Type      types.String `tfsdk:"type"`
	State     types.String `tfsdk:"state"`
	Owner     types.String `tfsdk:"owner"`
	CreatedAt types.String `tfsdk:"created_at"`
	UpdatedAt types.String `tfsdk:"updated_at"`
}

func (d *ObjectResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Object resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"namespace": schema.StringAttribute{
				Description: "Name of the Namespace",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"store": schema.StringAttribute{
				Description: "Name of the Store",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"sql": schema.StringAttribute{
				Description: "SQL statement to create the object",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the object",
				Optional:    true,
				Computed:    true,
			},
			"name": schema.StringAttribute{
				Description: "Name of the Object",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"fqn": schema.StringAttribute{
				Description: "Fully qualified name of the Object",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"type": schema.StringAttribute{
				Description: "Type of the Object",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"state": schema.StringAttribute{
				Description: "State of the Object",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the object",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Last update date of the object",
				Computed:    true,
			},
		},
	}
}

func (d *ObjectResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *ObjectResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_object"
}

type statementPlan struct {
	Ddl     *relationPlan  `json:"ddl,omitempty"`
	Sink    *relationPlan  `json:"sink,omitempty"`
	Sources []relationPlan `json:"sources,omitempty"`
}

type relationPlan struct {
	Fqn        string `json:"fqn"`
	Type       string `json:"type"`
	DbName     string `json:"db_name"`
	SchemaName string `json:"schema_name"`
	Name       string `json:"name"`
	StoreName  string `json:"store_name"`
}

type artifactDDL struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	Command string `json:"command"`
	Summary string `json:"summary"`
	Path    string `json:"path"`
}

// Create implements resource.Resource.
func (d *ObjectResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var object ObjectResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &object)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !object.Owner.IsNull() && !object.Owner.IsUnknown() {
		roleName = object.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if err := util.SetSqlContext(ctx, conn, object.Database.ValueStringPointer(), object.Namespace.ValueStringPointer(), object.Store.ValueStringPointer(), nil); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	dsql, err := util.ExecTemplate(describeObjectTmpl, map[string]any{
		"SQL": object.Sql.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	row := conn.QueryRowContext(ctx, dsql)
	var kind string
	var descJson string
	if err := row.Scan(&kind, &descJson); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create relation", err)
		return
	}

	if !util.ArrayContains([]string{kind}, []string{"CREATE_STREAM", "CREATE_CHANGELOG"}) {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("invalid relation type: %s", kind))
		return
	}

	statementPlan := statementPlan{}
	if err := json.Unmarshal([]byte(descJson), &statementPlan); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to parse relation plan", err)
		return
	}

	if statementPlan.Ddl == nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("invalid object plan"))
		return
	}

	if statementPlan.Ddl.DbName != object.Database.ValueString() {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("database name mismatch, statement would create object in %s instead of %s", statementPlan.Ddl.DbName, object.Database.ValueString()))
		return
	}

	if statementPlan.Ddl.SchemaName != object.Namespace.ValueString() {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("namespace name mismatch, statement would create object in %s instead of %s", statementPlan.Ddl.SchemaName, object.Namespace.ValueString()))
		return
	}

	if statementPlan.Ddl.StoreName != object.Store.ValueString() {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("store name mismatch, statement would use store %s instead of %s", statementPlan.Ddl.StoreName, object.Store.ValueString()))
		return
	}

	artifactDDL := artifactDDL{}
	row = conn.QueryRowContext(ctx, object.Sql.ValueString())
	if err := row.Scan(&artifactDDL.Type, &artifactDDL.Name, &artifactDDL.Command, &artifactDDL.Summary, &artifactDDL.Path); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create object", err)
		return
	}
	var pathArr []string
	if err := json.Unmarshal([]byte(artifactDDL.Path), &pathArr); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to parse object path", err)
		return
	}
	object.FQN = types.StringValue(fmt.Sprintf("%q.%q.%q", util.EscapeIdentifier(pathArr[0]), util.EscapeIdentifier(pathArr[1]), util.EscapeIdentifier(pathArr[2])))
	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		object, err = d.updateComputed(ctx, conn, object)
		if err != nil {
			return err
		}

		if object.State.ValueString() != "created" {
			return retry.RetryableError(fmt.Errorf("object not yet created"))
		}

		return nil
	}); err != nil {
		dsql, derr := util.ExecTemplate(dropObjectTmpl, map[string]any{
			"FQN": object.FQN.ValueString(),
		})
		if derr != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", derr)
			return
		}
		if _, derr := conn.ExecContext(ctx, dsql); derr != nil {
			tflog.Error(ctx, "failed to clean up object", map[string]any{
				"name":  object.FQN.ValueString(),
				"error": derr.Error(),
			})
		}
	}

	tflog.Info(ctx, "Object created", map[string]any{"name": object.FQN.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, object)...)
}

func (d *ObjectResource) updateComputed(ctx context.Context, conn *sql.Conn, object ObjectResourceData) (ObjectResourceData, error) {
	dsql, err := util.ExecTemplate(lookupObjectByFQNTmpl, map[string]any{
		"FQN": object.FQN.ValueString(),
	})
	if err != nil {
		return object, fmt.Errorf("failed to generate SQL: %w", err)
	}
	row := conn.QueryRowContext(ctx, dsql)
	if err := row.Err(); err != nil {
		return object, err
	}

	var (
		name      string
		kind      string
		owner     string
		state     string
		createdAt time.Time
		updatedAt time.Time
	)
	if err := row.Scan(&name, &kind, &owner, &state, &createdAt, &updatedAt); err != nil {
		return object, err
	}
	object.Name = types.StringValue(name)
	object.Owner = types.StringValue(owner)
	object.Type = types.StringValue(kind)
	object.State = types.StringValue(state)
	object.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	object.UpdatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	return object, nil
}

func (d *ObjectResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var object ObjectResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &object)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !object.Owner.IsNull() && !object.Owner.IsUnknown() {
		roleName = object.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(dropObjectTmpl, map[string]any{
		"FQN": object.FQN.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	if _, err := conn.ExecContext(ctx, dsql); err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidRelation {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to drop object", err)
			return
		}
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		dsql, err := util.ExecTemplate(checkObjectExistsByFQNTmpl, map[string]any{
			"FQN": object.FQN.ValueString(),
		})
		if err != nil {
			return fmt.Errorf("failed to generate SQL: %w", err)
		}
		row := conn.QueryRowContext(ctx, dsql)
		if err := row.Err(); err != nil {
			return err
		}

		var discard any
		if err := row.Scan(&discard); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}

		return retry.RetryableError(fmt.Errorf("object not yet deleted"))
	}); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to cleanup object", err)
		return
	}

	tflog.Info(ctx, "Object deleted", map[string]any{"name": object.FQN.ValueString()})
}

func (d *ObjectResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var currentObject ObjectResourceData
	var newObject ObjectResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newObject)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &currentObject)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	// all changes to database other than ownership are disallowed
	if !newObject.Database.Equal(currentObject.Database) || !newObject.Namespace.Equal(currentObject.Namespace) || !newObject.Store.Equal(currentObject.Store) {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid update", fmt.Errorf("database, namespace and store names cannot be changed"))
	}

	if !newObject.Owner.IsNull() && !newObject.Owner.IsUnknown() && newObject.Owner.Equal(currentObject.Owner) {
		// Transfer ownership
		tflog.Error(ctx, "transfer ownership not yet supported")
	}

	currentObject, err = d.updateComputed(ctx, conn, currentObject)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, currentObject)...)
}

func (d *ObjectResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var object ObjectResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &object)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	object, err = d.updateComputed(ctx, conn, object)
	if err != nil {
		var godsErr gods.ErrSQLError
		if errors.As(err, &godsErr) && godsErr.SQLCode == gods.SqlStateInvalidRelation {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, object)...)
}
