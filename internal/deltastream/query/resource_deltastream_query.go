// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package query

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

var _ resource.Resource = &QueryResource{}
var _ resource.ResourceWithConfigure = &QueryResource{}

func NewQueryResource() resource.Resource {
	return &QueryResource{}
}

type QueryResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type QueryResourceData struct {
	SourceRelations basetypes.ListValue   `tfsdk:"source_relation_fqns"`
	SinkRelation    basetypes.StringValue `tfsdk:"sink_relation_fqn"`
	Sql             basetypes.StringValue `tfsdk:"sql"`
	QueryID         basetypes.StringValue `tfsdk:"query_id"`
	State           basetypes.StringValue `tfsdk:"state"`
	Owner           basetypes.StringValue `tfsdk:"owner"`
	CreatedAt       basetypes.StringValue `tfsdk:"created_at"`
	UpdatedAt       basetypes.StringValue `tfsdk:"updated_at"`
}

func (d *QueryResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Query resource",

		Attributes: map[string]schema.Attribute{
			"source_relation_fqns": schema.ListAttribute{
				Description: "List of fully qualified source relation names",
				Required:    true,
				ElementType: basetypes.StringType{},
			},
			"sink_relation_fqn": schema.StringAttribute{
				Description: "Fully qualified sink relation name",
				Required:    true,
			},
			"sql": schema.StringAttribute{
				Description: "SQL statement to create the relation",
				Required:    true,
			},
			"query_id": schema.StringAttribute{
				Description: "Query ID",
				Computed:    true,
			},
			"owner": schema.StringAttribute{
				Description: "Owning role of the query",
				Optional:    true,
				Computed:    true,
				Validators:  util.IdentifierValidators,
			},
			"state": schema.StringAttribute{
				Description: "State of the Relation",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the query",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Creation date of the query",
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

	cfg, ok := req.ProviderData.(*config.DeltaStreamProviderCfg)
	if !ok {
		util.LogError(ctx, resp.Diagnostics, "internal error", fmt.Errorf("invalid provider data"))
		return
	}

	d.cfg = cfg
}

func (d *QueryResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_query"
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
func (d *QueryResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var query QueryResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &query)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	roleName := d.cfg.Role
	if !query.Owner.IsNull() && !query.Owner.IsUnknown() {
		roleName = query.Owner.ValueString()
	}

	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	row := conn.QueryRowContext(ctx, "DESCRIBE "+query.Sql.ValueString())
	var kind string
	var descJson string
	if err := row.Scan(&kind, &descJson); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to create relation", err)
		return
	}

	if !util.ArrayContains([]string{kind}, []string{"INSERT_INTO"}) {
		util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("invalid query type: %s", kind))
		return
	}

	statementPlan := statementPlan{}
	if err := json.Unmarshal([]byte(descJson), &statementPlan); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to parse query plan", err)
		return
	}

	if statementPlan.Ddl != nil {
		util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("invalid query plan"))
		return
	}

	if d.cfg.Organization+"."+strings.TrimSpace(query.SinkRelation.ValueString()) != statementPlan.Sink.Fqn {
		util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("sink relation mismatch %s != %s", query.SinkRelation.ValueString(), statementPlan.Sink.Fqn))
		return
	}

	var sourceRelations []string
	resp.Diagnostics.Append(query.SourceRelations.ElementsAs(ctx, &sourceRelations, false)...)
	if resp.Diagnostics.HasError() {
		return
	}
	for _, source := range statementPlan.Sources {
		found := false
		for _, sourceRelation := range sourceRelations {
			if d.cfg.Organization+"."+strings.TrimSpace(sourceRelation) == source.Fqn {
				found = true
				break
			}
		}
		if !found {
			util.LogError(ctx, resp.Diagnostics, "planning error", fmt.Errorf("query uses source relation %s but it is not specified as a source on the resource", source.Fqn))
			return
		}
	}

	artifactDDL := artifactDDL{}
	row = conn.QueryRowContext(ctx, query.Sql.ValueString())
	if err := row.Scan(&artifactDDL.Type, &artifactDDL.Name, &artifactDDL.Command, &artifactDDL.Summary); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to launch query", err)
		return
	}
	query.QueryID = basetypes.NewStringValue(artifactDDL.Name)

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		query, err = d.updateComputed(ctx, conn, query, false)
		if err != nil {
			return err
		}

		if query.State.ValueString() != "running" {
			return retry.RetryableError(fmt.Errorf("query not yet running"))
		}

		if query.State.ValueString() == "errored" {
			return fmt.Errorf("query in errored state")
		}

		return nil
	}); err != nil {
		util.LogError(ctx, resp.Diagnostics, "query failed to start", err)
		if _, derr := conn.ExecContext(ctx, fmt.Sprintf(`TERMINATE QUERY %s;`, query.QueryID.ValueString())); derr != nil {
			tflog.Error(ctx, "failed to clean up failed query", map[string]any{
				"Query ID": query.QueryID.ValueString(),
				"error":    derr.Error(),
			})
		}
		return
	}

	tflog.Info(ctx, "query created", map[string]any{"name": query.QueryID.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, query)...)
}

func (d *QueryResource) updateComputed(ctx context.Context, conn *sql.Conn, rel QueryResourceData, includeStopped bool) (QueryResourceData, error) {
	sql := `LIST QUERIES;`
	if includeStopped {
		sql = `LIST QUERIES WITH ('all');`
	}

	rows, err := conn.QueryContext(ctx, sql)
	if err != nil {
		return rel, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id            string
			intendedState string
			actualState   string
			query         string
			owner         string
			createdAt     time.Time
			updatedAt     time.Time
		)

		if err := rows.Scan(&id, &intendedState, &actualState, &query, &owner, &createdAt, &updatedAt); err != nil {
			return rel, err
		}
		if id == rel.QueryID.ValueString() {
			rel.QueryID = basetypes.NewStringValue(id)
			rel.State = basetypes.NewStringValue(actualState)
			rel.Owner = basetypes.NewStringValue(owner)
			rel.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			rel.UpdatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			return rel, nil
		}
	}
	return rel, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidQuery}
}

func (d *QueryResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var query QueryResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &query)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	roleName := d.cfg.Role
	if !query.Owner.IsNull() && !query.Owner.IsUnknown() {
		roleName = query.Owner.ValueString()
	}
	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`TERMINATE QUERY %s;`, query.QueryID.ValueString())); err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidQuery {
			util.LogError(ctx, resp.Diagnostics, "failed to terminate query", err)
			return
		}
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		query, err = d.updateComputed(ctx, conn, query, true)
		if err != nil {
			return err
		}

		if query.State.ValueString() == "stopped" {
			return nil
		}

		return retry.RetryableError(fmt.Errorf("query not yet terminated"))
	}); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to cleanup relation", err)
		return
	}

	tflog.Info(ctx, "Query terminated", map[string]any{"name": query.QueryID.ValueString()})
}

func (d *QueryResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var currentQuery QueryResourceData
	var newQuery QueryResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newQuery)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &currentQuery)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if !newQuery.Owner.IsNull() && !newQuery.Owner.IsUnknown() && newQuery.Owner.Equal(currentQuery.Owner) {
		// Transfer ownership
		tflog.Error(ctx, "transfer ownership not yet supported")
	}

	util.LogError(ctx, resp.Diagnostics, "invalid update", fmt.Errorf("query properties cannot be changed"))

	currentQuery, err = d.updateComputed(ctx, conn, currentQuery, false)
	if err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, currentQuery)...)
}

func (d *QueryResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var query QueryResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &query)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	query, err = d.updateComputed(ctx, conn, query, true)
	if err != nil {
		var godsErr gods.ErrSQLError
		if errors.As(err, &godsErr) && godsErr.SQLCode == gods.SqlStateInvalidQuery {
			return
		}
		util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, query)...)
}
