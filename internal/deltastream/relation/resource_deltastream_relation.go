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
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
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
	Database basetypes.StringValue `tfsdk:"database"`
	Schema   basetypes.StringValue `tfsdk:"schema"`
	Name     basetypes.StringValue `tfsdk:"name"`
	Store    basetypes.StringValue `tfsdk:"store"`
	Sql      basetypes.StringValue `tfsdk:"sql"`

	FQN       basetypes.StringValue `tfsdk:"fqn"`
	Type      basetypes.StringValue `tfsdk:"type"`
	State     basetypes.StringValue `tfsdk:"state"`
	Owner     basetypes.StringValue `tfsdk:"owner"`
	CreatedAt basetypes.StringValue `tfsdk:"created_at"`
	UpdatedAt basetypes.StringValue `tfsdk:"updated_at"`
}

func (d *RelationResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Relation resource",

		Attributes: map[string]schema.Attribute{
			"database": schema.StringAttribute{
				Description: "Name of the Database",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"schema": schema.StringAttribute{
				Description: "Name of the Schema",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"store": schema.StringAttribute{
				Description: "Name of the Store",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"sql": schema.StringAttribute{
				Description: "SQL statement to create the relation",
				Required:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the Database",
				Optional:    true,
				Computed:    true,
				Validators:  util.IdentifierValidators,
			},

			"name": schema.StringAttribute{
				Description: "Name of the Relation",
				Computed:    true,
				Validators:  util.IdentifierValidators,
			},
			"fqn": schema.StringAttribute{
				Description: "Fully qualified name of the Relation",
				Computed:    true,
			},
			"type": schema.StringAttribute{
				Description: "Type of the Relation",
				Computed:    true,
			},
			"state": schema.StringAttribute{
				Description: "State of the Relation",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the Database",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Creation date of the Database",
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
		resp.Diagnostics.AddError("internal error", "invalid provider data")
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

	conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect to database", err.Error())
		return
	}
	defer conn.Close()

	roleName := d.cfg.Role
	if !relation.Owner.IsNull() && !relation.Owner.IsUnknown() {
		roleName = relation.Owner.ValueString()
	}

	if err := util.SetSqlContext(ctx, conn, &roleName, relation.Database.ValueStringPointer(), relation.Schema.ValueStringPointer(), relation.Store.ValueStringPointer()); err != nil {
		resp.Diagnostics.AddError("failed to set sql context", err.Error())
		return
	}

	row := conn.QueryRowContext(ctx, "DESCRIBE "+relation.Sql.ValueString())
	var kind string
	var descJson string
	if err := row.Scan(&kind, &descJson); err != nil {
		resp.Diagnostics.AddError("failed to create relation", err.Error())
		return
	}

	if !util.ArrayContains([]string{kind}, []string{"CREATE_STREAM", "CREATE_CHANGELOG", "CREATE_INDEX", "CREATE_TABLE"}) {
		resp.Diagnostics.AddError("planning error", "invalid relation type: "+kind)
		return
	}

	statementPlan := statementPlan{}
	if err := json.Unmarshal([]byte(descJson), &statementPlan); err != nil {
		resp.Diagnostics.AddError("failed to parse relation plan", err.Error())
		return
	}

	if statementPlan.Ddl == nil {
		resp.Diagnostics.AddError("planning error", "invalid relation plan")
		return
	}

	if statementPlan.Ddl.DbName != relation.Database.ValueString() {
		resp.Diagnostics.AddError("planning error", "database name mismatch, statement would create relation in "+statementPlan.Ddl.DbName+" instead of "+relation.Database.ValueString())
		return
	}

	if statementPlan.Ddl.SchemaName != relation.Schema.ValueString() {
		resp.Diagnostics.AddError("planning error", "schema name mismatch, statement would create relation in "+statementPlan.Ddl.SchemaName+" instead of "+relation.Schema.ValueString())
		return
	}

	if statementPlan.Ddl.StoreName != relation.Store.ValueString() {
		resp.Diagnostics.AddError("planning error", "store name mismatch, statement would use store "+statementPlan.Ddl.StoreName+" instead of "+relation.Store.ValueString())
		return
	}

	artifactDDL := artifactDDL{}
	row = conn.QueryRowContext(ctx, relation.Sql.ValueString())
	if err := row.Scan(&artifactDDL.Type, &artifactDDL.Name, &artifactDDL.Command, &artifactDDL.Summary); err != nil {
		resp.Diagnostics.AddError("failed to create relation", err.Error())
		return
	}
	relation.FQN = basetypes.NewStringValue(artifactDDL.Name)

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
		if _, derr := conn.ExecContext(ctx, fmt.Sprintf(`DROP RELATION %s;`, relation.FQN.ValueString())); derr != nil {
			tflog.Error(ctx, "failed to clean up schema", map[string]any{
				"name":  relation.FQN.ValueString(),
				"error": derr.Error(),
			})
		}
	}

	tflog.Info(ctx, "Relation created", map[string]any{"name": relation.FQN.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, relation)...)
}

func (d *RelationResource) updateComputed(ctx context.Context, conn *sql.Conn, rel RelationResourceData) (RelationResourceData, error) {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`LIST RELATIONS IN SCHEMA "%s"."%s";`, rel.Database.ValueString(), rel.Schema.ValueString()))
	if err != nil {
		return rel, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name           string
			kind           string
			owner          string
			state          string
			propertiesJSON string
			createdAt      time.Time
			updatedAt      time.Time
		)

		if err := rows.Scan(&name, &kind, &owner, &state, &propertiesJSON, &createdAt, &updatedAt); err != nil {
			return rel, err
		}
		if fmt.Sprintf("%s.%s.%s", rel.Database.ValueString(), rel.Schema.ValueString(), name) == rel.FQN.ValueString() {
			rel.Name = basetypes.NewStringValue(name)
			rel.Type = basetypes.NewStringValue(kind)
			rel.State = basetypes.NewStringValue(state)
			rel.Owner = basetypes.NewStringValue(owner)
			rel.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			rel.UpdatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			return rel, nil
		}
	}
	return rel, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidRelation}
}

func (d *RelationResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var relation RelationResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &relation)...)
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
	if !relation.Owner.IsNull() && !relation.Owner.IsUnknown() {
		roleName = relation.Owner.ValueString()
	}
	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		resp.Diagnostics.AddError("failed to set sql context", err.Error())
		return
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP RELATION %s;`, relation.FQN.ValueString())); err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidDatabase {
			resp.Diagnostics.AddError("failed to drop relation", err.Error())
			return
		}
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DESCRIBE RELATION %s;`, relation.FQN.ValueString())); err != nil {
			var sqlErr gods.ErrSQLError
			if errors.As(err, &sqlErr) && sqlErr.SQLCode == gods.SqlStateInvalidRelation {
				return nil
			}
			return err
		}

		return retry.RetryableError(fmt.Errorf("relation not yet deleted"))
	}); err != nil {
		resp.Diagnostics.AddError("failed to cleanup relation", err.Error())
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

	conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect to database", err.Error())
		return
	}
	defer conn.Close()

	// all changes to database other than ownership are disallowed
	if !newRelation.Database.Equal(currentRelation.Database) || !newRelation.Schema.Equal(currentRelation.Schema) || !newRelation.Store.Equal(currentRelation.Store) {
		resp.Diagnostics.AddError("invalid update", "database, schema and store names cannot be changed")
	}

	if !newRelation.Owner.IsNull() && !newRelation.Owner.IsUnknown() && newRelation.Owner.Equal(currentRelation.Owner) {
		// Transfer ownership
		tflog.Error(ctx, "transfer ownership not yet supported")
	}

	currentRelation, err = d.updateComputed(ctx, conn, currentRelation)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
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

	conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics.AddError("failed to connect to database", err.Error())
		return
	}
	defer conn.Close()

	relation, err = d.updateComputed(ctx, conn, relation)
	if err != nil {
		resp.Diagnostics.AddError("failed to update state", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, relation)...)
}
