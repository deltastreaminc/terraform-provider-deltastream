// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/hashicorp/terraform-plugin-log/tflog"

	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
)

var _ resource.Resource = &EntityResource{}
var _ resource.ResourceWithConfigure = &EntityResource{}

func NewEntityResource() resource.Resource {
	return &EntityResource{}
}

type EntityResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type EntityResourceData struct {
	Store                basetypes.StringValue `tfsdk:"store"`
	EntityPath           basetypes.ListValue   `tfsdk:"entity_path"`
	KafkaProperties      basetypes.ObjectValue `tfsdk:"kafka_properties"`
	KinesisProperties    basetypes.ObjectValue `tfsdk:"kinesis_properties"`
	DatabricksProperties basetypes.ObjectValue `tfsdk:"databricks_properties"`
	SnowflakeProperties  basetypes.ObjectValue `tfsdk:"snowflake_properties"`
	PostgresProperties   basetypes.ObjectValue `tfsdk:"postgres_properties"`
}

type KafkaStoreEntityResourceData struct {
	TopicPartitions basetypes.Int64Value  `tfsdk:"topic_partitions"`
	TopicReplicas   basetypes.Int64Value  `tfsdk:"topic_replicas"`
	KeyDescriptor   basetypes.StringValue `tfsdk:"key_descriptor"`
	ValueDescriptor basetypes.StringValue `tfsdk:"value_descriptor"`
	Configs         basetypes.MapValue    `tfsdk:"configs"`
}

func (KafkaStoreEntityResourceData) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"topic_partitions": basetypes.Int64Type{},
		"topic_replicas":   basetypes.Int64Type{},
		"key_descriptor":   basetypes.StringType{},
		"value_descriptor": basetypes.StringType{},
		"configs": basetypes.MapType{
			ElemType: basetypes.StringType{},
		},
	}
}

type KinesisStoreEntityResourceData struct {
	KinesisShards basetypes.Int64Value  `tfsdk:"kinesis_shards"`
	Descriptor    basetypes.StringValue `tfsdk:"descriptor"`
}

func (KinesisStoreEntityResourceData) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"kinesis_shards": basetypes.Int64Type{},
		"descriptor":     basetypes.StringType{},
	}
}

type SnowflakeStoreEntityResourceData struct {
	Details basetypes.MapValue `tfsdk:"details"`
}

func (SnowflakeStoreEntityResourceData) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"details": basetypes.MapType{
			ElemType: basetypes.StringType{},
		},
	}
}

type DatabricksStoreEntityResourceData struct {
	Details basetypes.MapValue `tfsdk:"details"`
}

func (DatabricksStoreEntityResourceData) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"details": basetypes.MapType{
			ElemType: basetypes.StringType{},
		},
	}
}

type PostgresStoreEntityResourceData struct {
	Details basetypes.MapValue `tfsdk:"details"`
}

func (PostgresStoreEntityResourceData) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"details": basetypes.MapType{
			ElemType: basetypes.StringType{},
		},
	}
}

func (d *EntityResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Database resource",

		Attributes: map[string]schema.Attribute{
			"store": schema.StringAttribute{
				Description: "Store name",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"entity_path": schema.ListAttribute{
				Description: "Entity path",
				Required:    true,
				ElementType: basetypes.StringType{},
			},
			"kafka_properties": schema.SingleNestedAttribute{
				Description: "Kafka properties",
				Attributes: map[string]schema.Attribute{
					"topic_partitions": schema.Int64Attribute{
						Description: "Number of partitions",
						Optional:    true,
						Computed:    true,
					},
					"topic_replicas": schema.Int64Attribute{
						Description: "Number of replicas",
						Optional:    true,
						Computed:    true,
					},
					"key_descriptor": schema.StringAttribute{
						Description: "Protobuf descriptor for key",
						Optional:    true,
						Computed:    true,
					},
					"value_descriptor": schema.StringAttribute{
						Description: "Protobuf descriptor for value",
						Optional:    true,
						Computed:    true,
					},
					"configs": schema.MapAttribute{
						Description: "Additional topic configurations",
						Optional:    true,
						Computed:    true,
						ElementType: basetypes.StringType{},
					},
				},
				Optional: true,
				Computed: true,
			},
			"kinesis_properties": schema.SingleNestedAttribute{
				Description: "Kinesis properties",
				Attributes: map[string]schema.Attribute{
					"kinesis_shards": schema.Int64Attribute{
						Description: "Number of shards",
						Optional:    true,
						Computed:    true,
					},
					"descriptor": schema.StringAttribute{
						Description: "Protobuf descriptor for the value",
						Optional:    true,
						Computed:    true,
					},
				},
				Optional: true,
				Computed: true,
			},
			"snowflake_properties": schema.SingleNestedAttribute{
				Description: "Snowflake properties",
				Attributes: map[string]schema.Attribute{
					"details": schema.MapAttribute{
						ElementType: basetypes.StringType{},
						Computed:    true,
					},
				},
				Optional: true,
				Computed: true,
			},
			"databricks_properties": schema.SingleNestedAttribute{
				Description: "Databricks properties",
				Attributes: map[string]schema.Attribute{
					"details": schema.MapAttribute{
						ElementType: basetypes.StringType{},
						Computed:    true,
					},
				},
				Optional: true,
				Computed: true,
			},
			"postgres_properties": schema.SingleNestedAttribute{
				Description: "Postgres properties",
				Attributes: map[string]schema.Attribute{
					"details": schema.MapAttribute{
						ElementType: basetypes.StringType{},
						Computed:    true,
					},
				},
				Optional: true,
				Computed: true,
			},
		},
	}
}

func (d *EntityResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *EntityResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_entity"
}

var createEntityStatement = `
	CREATE ENTITY {{ range $index, $element := .EntityPath }}
        {{if $index}}.{{end}}
        {{- $element}}
    {{ end }}
	IN STORE {{ .StoreName }}
	{{ if .Properties }}WITH ( {{ .Properties }} ){{ end }}
	;
`

// Create implements resource.Resource.
func (d *EntityResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var entity EntityResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &entity)...)
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
	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	entityPath := []string{}
	resp.Diagnostics.Append(entity.EntityPath.ElementsAs(ctx, &entityPath, false)...)
	if resp.Diagnostics.HasError() {
		return
	}

	storeType, err := getStoreType(ctx, conn, entity.Store.ValueString())
	if err != nil {
		util.LogError(ctx, resp.Diagnostics, "invalid store type", err)
		return
	}

	properties := []string{}
	switch storeType {
	case "Kafka":
		fallthrough
	case "ConfluentKakfa":
		var kafkaProperties KafkaStoreEntityResourceData
		if !entity.KafkaProperties.IsNull() && !entity.KafkaProperties.IsUnknown() {
			resp.Diagnostics.Append(entity.KafkaProperties.As(ctx, &kafkaProperties, basetypes.ObjectAsOptions{})...)
			if resp.Diagnostics.HasError() {
				return
			}
		}
		if !kafkaProperties.TopicPartitions.IsNull() && !kafkaProperties.TopicPartitions.IsUnknown() {
			properties = append(properties, fmt.Sprintf("'kafka.partitions' = %d", kafkaProperties.TopicPartitions.ValueInt64()))
		}
		if !kafkaProperties.TopicReplicas.IsNull() && !kafkaProperties.TopicReplicas.IsUnknown() {
			properties = append(properties, fmt.Sprintf("'kafka.replicas' = %d", kafkaProperties.TopicReplicas.ValueInt64()))
		}
		for k, v := range kafkaProperties.Configs.Elements() {
			properties = append(properties, fmt.Sprintf("'%s' = '%s'", k, v.(basetypes.StringValue).ValueString()))
		}
	case "Kinesis":
		var kinesisProperties KinesisStoreEntityResourceData
		if !entity.KinesisProperties.IsNull() && !entity.KinesisProperties.IsUnknown() {
			resp.Diagnostics.Append(entity.KinesisProperties.As(ctx, &kinesisProperties, basetypes.ObjectAsOptions{})...)
			if resp.Diagnostics.HasError() {
				return
			}
		}
		if !kinesisProperties.KinesisShards.IsNull() && !kinesisProperties.KinesisShards.IsUnknown() {
			properties = append(properties, fmt.Sprintf("'kinesis.shards' = %d", kinesisProperties.KinesisShards.ValueInt64()))
		}
	}

	b := bytes.NewBuffer(nil)
	template.Must(template.New("").Parse(createEntityStatement)).Execute(b, map[string]any{
		"StoreName":  entity.Store.ValueString(),
		"EntityPath": entityPath,
		"Properties": strings.Join(properties, ", "),
	})
	sql := b.String()
	if _, err := conn.ExecContext(ctx, sql); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to create entity", err)
		return
	}

	resp.Diagnostics.Append(d.updateComputed(ctx, &entity)...)
	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Info(ctx, "Entity created", map[string]any{"store": entity.Store.String(), "name": entity.EntityPath.String()})
	resp.Diagnostics.Append(resp.State.Set(ctx, entity)...)
}

const dropEntityStatement = `DROP ENTITY 	
	{{ range $index, $element := .EntityPath }}
		{{ if $index }}.{{ end }}
		{{- $element }}
	{{ end }}
	IN STORE {{ .StoreName }};
`

func (d *EntityResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var entity EntityResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &entity)...)
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
	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	entityPath := []string{}
	resp.Diagnostics.Append(entity.EntityPath.ElementsAs(ctx, &entityPath, false)...)
	if resp.Diagnostics.HasError() {
		return
	}

	b := bytes.NewBuffer(nil)
	template.Must(template.New("").Parse(dropEntityStatement)).Execute(b, map[string]any{
		"StoreName":  entity.Store.ValueString(),
		"EntityPath": entityPath,
	})
	if _, err := conn.ExecContext(ctx, b.String()); err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to create database", err)
		return
	}
	tflog.Info(ctx, "Entity deleted", map[string]any{"store": entity.Store.String(), "name": entity.EntityPath.String()})
}

func (d *EntityResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var currentEntity EntityResourceData
	var newEntity EntityResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newEntity)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &currentEntity)...)
	if resp.Diagnostics.HasError() {
		return
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	util.LogError(ctx, resp.Diagnostics, "invalid update", fmt.Errorf("entity cannot be changed"))
	resp.Diagnostics.Append(resp.State.Set(ctx, currentEntity)...)
}

func (d *EntityResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var entity EntityResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &entity)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(d.updateComputed(ctx, &entity)...)
	if resp.Diagnostics.HasError() {
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, entity)...)
}

func (d *EntityResource) updateComputed(ctx context.Context, entity *EntityResourceData) (diags diag.Diagnostics) {
	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		diags.AddError("failed to connect", err.Error())
		return
	}
	defer conn.Close()

	roleName := d.cfg.Role
	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		diags.AddError("failed to set sql context", err.Error())
		return
	}

	entityPath := []string{}
	diags.Append(entity.EntityPath.ElementsAs(ctx, &entityPath, false)...)
	if diags.HasError() {
		return
	}

	storeType, err := getStoreType(ctx, conn, entity.Store.ValueString())
	if err != nil {
		diags.AddError(err.Error(), "")
		return
	}

	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`DESCRIBE ENTITY %s IN STORE %s;`, strings.Join(entityPath, "."), entity.Store.ValueString()))
	if err != nil {
		diags.AddError("failed to describe entity", err.Error())
		return
	}
	defer rows.Close()

	if !rows.Next() {
		diags.AddError("entity not found", "")
		return
	}
	switch storeType {
	case "Kafka":
		fallthrough
	case "ConfluentKafka":
		var discard any
		var topicPartitions int64
		var topicReplicas int64
		var keyDescriptor *string
		var valueDescriptor *string
		var configJSON string
		if err := rows.Scan(&discard, &topicPartitions, &topicReplicas, &keyDescriptor, &valueDescriptor, &configJSON); err != nil {
			diags.AddError("failed to read entity", err.Error())
			return
		}
		var kafkaProperties KafkaStoreEntityResourceData
		kafkaProperties.TopicPartitions = basetypes.NewInt64Value(topicPartitions)
		kafkaProperties.TopicReplicas = basetypes.NewInt64Value(topicReplicas)
		kafkaProperties.KeyDescriptor = basetypes.NewStringPointerValue(keyDescriptor)
		kafkaProperties.ValueDescriptor = basetypes.NewStringPointerValue(valueDescriptor)
		configs := map[string]string{}
		if err := json.Unmarshal([]byte(configJSON), &configs); err != nil {
			diags.AddError("failed to read entity configuration", err.Error())
			return
		}
		var d diag.Diagnostics
		kafkaProperties.Configs, d = basetypes.NewMapValueFrom(ctx, basetypes.StringType{}, configs)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		entity.KafkaProperties, d = basetypes.NewObjectValueFrom(ctx, kafkaProperties.AttributeTypes(), kafkaProperties)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
	case "Kinisis":
		var discard any
		var topicShards int64
		var descriptor string
		if err := rows.Scan(&discard, &topicShards, &descriptor); err != nil {
			diags.AddError("failed to read entity", err.Error())
			return
		}
		var kinesisProperties KinesisStoreEntityResourceData
		kinesisProperties.KinesisShards = basetypes.NewInt64Value(topicShards)
		kinesisProperties.Descriptor = basetypes.NewStringValue(descriptor)
		var d diag.Diagnostics
		entity.KinesisProperties, d = basetypes.NewObjectValueFrom(ctx, kinesisProperties.AttributeTypes(), kinesisProperties)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		entity.KinesisProperties, d = basetypes.NewObjectValueFrom(ctx, kinesisProperties.AttributeTypes(), kinesisProperties)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
	case "Snowflake":
		detail, err := rowsToMap(rows)
		if err != nil {
			diags.AddError("failed to read entity", err.Error())
			return
		}
		var snowflakeProperties SnowflakeStoreEntityResourceData
		var d diag.Diagnostics
		snowflakeProperties.Details, d = basetypes.NewMapValueFrom(ctx, basetypes.StringType{}, detail)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		entity.SnowflakeProperties, d = basetypes.NewObjectValueFrom(ctx, snowflakeProperties.AttributeTypes(), snowflakeProperties)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
	case "Databricks":
		detail, err := rowsToMap(rows)
		if err != nil {
			diags.AddError("failed to read entity", err.Error())
			return
		}
		var databricksProperties DatabricksStoreEntityResourceData
		var d diag.Diagnostics
		databricksProperties.Details, d = basetypes.NewMapValueFrom(ctx, basetypes.StringType{}, detail)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		entity.SnowflakeProperties, d = basetypes.NewObjectValueFrom(ctx, databricksProperties.AttributeTypes(), databricksProperties)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
	case "Postgres":
		detail, err := rowsToMap(rows)
		if err != nil {
			diags.AddError("failed to read entity", err.Error())
			return
		}
		var postgresProperties PostgresStoreEntityResourceData
		var d diag.Diagnostics
		postgresProperties.Details, d = basetypes.NewMapValueFrom(ctx, basetypes.StringType{}, detail)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		entity.SnowflakeProperties, d = basetypes.NewObjectValueFrom(ctx, postgresProperties.AttributeTypes(), postgresProperties)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
	}

	if entity.KafkaProperties.IsUnknown() {
		entity.KafkaProperties = basetypes.NewObjectNull(KafkaStoreEntityResourceData{}.AttributeTypes())
	}
	if entity.KinesisProperties.IsUnknown() {
		entity.KinesisProperties = basetypes.NewObjectNull(KinesisStoreEntityResourceData{}.AttributeTypes())
	}
	if entity.SnowflakeProperties.IsUnknown() {
		entity.SnowflakeProperties = basetypes.NewObjectNull(SnowflakeStoreEntityResourceData{}.AttributeTypes())
	}
	if entity.DatabricksProperties.IsUnknown() {
		entity.DatabricksProperties = basetypes.NewObjectNull(DatabricksStoreEntityResourceData{}.AttributeTypes())
	}
	if entity.PostgresProperties.IsUnknown() {
		entity.PostgresProperties = basetypes.NewObjectNull(PostgresStoreEntityResourceData{}.AttributeTypes())
	}

	return
}

func getStoreType(ctx context.Context, conn *sql.Conn, storeName string) (string, error) {
	rows, err := conn.QueryContext(ctx, `LIST STORES;`)
	if err != nil {
		return "", fmt.Errorf("failed to list stores: %w", err)
	}
	defer rows.Close()

	var storeType string
	for rows.Next() {
		var discard any
		var name string
		var kind string
		if err := rows.Scan(&name, &kind, &discard, &discard, &discard, &discard, &discard, &discard); err != nil {
			return "", fmt.Errorf("failed to read store: %w", err)
		}
		if name == storeName {
			storeType = kind
			break
		}
	}
	if storeType == "" {
		return "", fmt.Errorf("store not found: %s", storeName)
	}
	return storeType, nil
}

func rowsToMap(rows *sql.Rows) (map[string]string, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	data := make([]string, len(cols))
	dataArr := make([]any, len(data))
	for i := range data {
		dataArr[i] = &data[i]
	}
	if err := rows.Scan(dataArr...); err != nil {
		return nil, err
	}
	details := map[string]string{}
	for i, col := range cols {
		details[col] = data[i]
	}
	return details, nil
}
