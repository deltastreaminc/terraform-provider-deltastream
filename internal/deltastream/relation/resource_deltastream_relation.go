// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package relation

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

var _ resource.Resource = &RelationResource{}
var _ resource.ResourceWithConfigure = &RelationResource{}

func NewRelationResource() resource.Resource {
	return &RelationResource{}
}

type RelationResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type RelationResourceData struct {
	Database types.String `tfsdk:"database"`
	Schema   types.String `tfsdk:"schema"`
	Name     types.String `tfsdk:"name"`
	Store    types.String `tfsdk:"store"`
	Sql      types.String `tfsdk:"sql"`

	FQN       types.String `tfsdk:"fqn"`
	Type      types.String `tfsdk:"type"`
	State     types.String `tfsdk:"state"`
	Owner     types.String `tfsdk:"owner"`
	CreatedAt types.String `tfsdk:"created_at"`
	UpdatedAt types.String `tfsdk:"updated_at"`
}

func (d *RelationResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Relation resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"schema": schema.StringAttribute{
				Description: "Name of the Schema",
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
				Description: "SQL statement to create the relation",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the relation",
				Optional:    true,
				Computed:    true,
			},
			"name": schema.StringAttribute{
				Description: "Name of the Relation",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"fqn": schema.StringAttribute{
				Description: "Fully qualified name of the Relation",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"type": schema.StringAttribute{
				Description: "Type of the Relation",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"state": schema.StringAttribute{
				Description: "State of the Relation",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the relation",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Creation date of the relation",
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

	cfg, ok := req.ProviderData.(*config.DeltaStreamProviderCfg)
	if !ok {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "internal error", fmt.Errorf("invalid provider data"))
		return
	}

	d.cfg = cfg
}

func (d *RelationResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_relation"
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
}

// Create implements resource.Resource.
func (d *RelationResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var relation RelationResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &relation)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !relation.Owner.IsNull() && !relation.Owner.IsUnknown() {
		roleName = relation.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if err := util.SetSqlContext(ctx, conn, relation.Database.ValueStringPointer(), relation.Schema.ValueStringPointer(), relation.Store.ValueStringPointer()); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	dsql, err := util.ExecTemplate(describeRelationTmpl, map[string]any{
		"SQL": relation.Sql.ValueString(),
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
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("invalid relation plan"))
		return
	}

	if statementPlan.Ddl.DbName != relation.Database.ValueString() {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("database name mismatch, statement would create relation in %s instead of %s", statementPlan.Ddl.DbName, relation.Database.ValueString()))
		return
	}

	if statementPlan.Ddl.SchemaName != relation.Schema.ValueString() {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("schema name mismatch, statement would create relation in %s instead of %s", statementPlan.Ddl.SchemaName, relation.Schema.ValueString()))
		return
	}

	if statementPlan.Ddl.StoreName != relation.Store.ValueString() {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("store name mismatch, statement would use store %s instead of %s", statementPlan.Ddl.StoreName, relation.Store.ValueString()))
		return
	}

	artifactDDL := artifactDDL{}
	row = conn.QueryRowContext(ctx, relation.Sql.ValueString())
	if err := row.Scan(&artifactDDL.Type, &artifactDDL.Name, &artifactDDL.Command, &artifactDDL.Summary); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create relation", err)
		return
	}
	relation.FQN = types.StringValue(artifactDDL.Name)
	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		relation, err = d.updateComputed(ctx, conn, relation)
		if err != nil {
			return err
		}

		if relation.State.ValueString() != "created" {
			return retry.RetryableError(fmt.Errorf("relation not yet created"))
		}

		return nil
	}); err != nil {
		dsql, derr := util.ExecTemplate(describeRelationTmpl, map[string]any{
			"FQN": relation.FQN.ValueString(),
		})
		if derr != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", derr)
			return
		}
		if _, derr := conn.ExecContext(ctx, dsql); derr != nil {
			tflog.Error(ctx, "failed to clean up relation", map[string]any{
				"name":  relation.FQN.ValueString(),
				"error": derr.Error(),
			})
		}
	}

	tflog.Info(ctx, "Relation created", map[string]any{"name": relation.FQN.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, relation)...)
}

func (d *RelationResource) updateComputed(ctx context.Context, conn *sql.Conn, relation RelationResourceData) (RelationResourceData, error) {
	dsql, err := util.ExecTemplate(lookupRelationByFQNTmpl, map[string]any{
		"FQN": relation.FQN.ValueString(),
	})
	if err != nil {
		return relation, fmt.Errorf("failed to generate SQL: %w", err)
	}
	row := conn.QueryRowContext(ctx, dsql)
	if err := row.Err(); err != nil {
		return relation, err
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
		return relation, err
	}
	relation.Name = types.StringValue(name)
	relation.Owner = types.StringValue(owner)
	relation.Type = types.StringValue(kind)
	relation.State = types.StringValue(state)
	relation.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	relation.UpdatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	return relation, nil
}

func (d *RelationResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var relation RelationResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &relation)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !relation.Owner.IsNull() && !relation.Owner.IsUnknown() {
		roleName = relation.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	dsql, err := util.ExecTemplate(dropRelationTmpl, map[string]any{
		"FQN": relation.FQN.ValueString(),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	if _, err := conn.ExecContext(ctx, dsql); err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidRelation {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to drop relation", err)
			return
		}
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		dsql, err := util.ExecTemplate(checkRelationExistsByFQNTmpl, map[string]any{
			"FQN": relation.FQN.ValueString(),
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

		return retry.RetryableError(fmt.Errorf("relation not yet deleted"))
	}); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to cleanup relation", err)
		return
	}

	tflog.Info(ctx, "Relation deleted", map[string]any{"name": relation.FQN.ValueString()})
}

func (d *RelationResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var currentRelation RelationResourceData
	var newRelation RelationResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newRelation)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &currentRelation)...)
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
	if !newRelation.Database.Equal(currentRelation.Database) || !newRelation.Schema.Equal(currentRelation.Schema) || !newRelation.Store.Equal(currentRelation.Store) {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid update", fmt.Errorf("database, schema and store names cannot be changed"))
	}

	if !newRelation.Owner.IsNull() && !newRelation.Owner.IsUnknown() && newRelation.Owner.Equal(currentRelation.Owner) {
		// Transfer ownership
		tflog.Error(ctx, "transfer ownership not yet supported")
	}

	currentRelation, err = d.updateComputed(ctx, conn, currentRelation)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, currentRelation)...)
}

func (d *RelationResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var relation RelationResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &relation)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	relation, err = d.updateComputed(ctx, conn, relation)
	if err != nil {
		var godsErr gods.ErrSQLError
		if errors.As(err, &godsErr) && godsErr.SQLCode == gods.SqlStateInvalidRelation {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, relation)...)
}
