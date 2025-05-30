// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/mapplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
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
	Store               types.String `tfsdk:"store"`
	EntityPath          types.List   `tfsdk:"entity_path"`
	KafkaProperties     types.Object `tfsdk:"kafka_properties"`
	KinesisProperties   types.Object `tfsdk:"kinesis_properties"`
	SnowflakeProperties types.Object `tfsdk:"snowflake_properties"`
	PostgresProperties  types.Object `tfsdk:"postgres_properties"`
}

type KafkaStoreEntityResourceData struct {
	TopicPartitions types.Int64  `tfsdk:"topic_partitions"`
	TopicReplicas   types.Int64  `tfsdk:"topic_replicas"`
	KeyDescriptor   types.String `tfsdk:"key_descriptor"`
	ValueDescriptor types.String `tfsdk:"value_descriptor"`
	Configs         types.Map    `tfsdk:"configs"`
	AllConfigs      types.Map    `tfsdk:"all_configs"`
}

func (KafkaStoreEntityResourceData) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"topic_partitions": types.Int64Type,
		"topic_replicas":   types.Int64Type,
		"key_descriptor":   types.StringType,
		"value_descriptor": types.StringType,
		"configs": types.MapType{
			ElemType: types.StringType,
		},
		"all_configs": types.MapType{
			ElemType: types.StringType,
		},
	}
}

type KinesisStoreEntityResourceData struct {
	KinesisShards types.Int64  `tfsdk:"kinesis_shards"`
	Descriptor    types.String `tfsdk:"descriptor"`
}

func (KinesisStoreEntityResourceData) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"kinesis_shards": types.Int64Type,
		"descriptor":     types.StringType,
	}
}

type SnowflakeStoreEntityResourceData struct {
	Details types.Map `tfsdk:"details"`
}

func (SnowflakeStoreEntityResourceData) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"details": types.MapType{
			ElemType: types.StringType,
		},
	}
}

type PostgresStoreEntityResourceData struct {
	Details types.Map `tfsdk:"details"`
}

func (PostgresStoreEntityResourceData) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"details": basetypes.MapType{
			ElemType: types.StringType,
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
			},
			"entity_path": schema.ListAttribute{
				Description: "Entity path",
				Required:    true,
				ElementType: types.StringType,
			},
			"kafka_properties": schema.SingleNestedAttribute{
				Description: "Kafka properties",
				Attributes: map[string]schema.Attribute{
					"topic_partitions": schema.Int64Attribute{
						Description: "Number of partitions",
						Optional:    true,
						Computed:    true,
						PlanModifiers: []planmodifier.Int64{
							int64planmodifier.RequiresReplace(),
						},
					},
					"topic_replicas": schema.Int64Attribute{
						Description: "Number of replicas",
						Optional:    true,
						Computed:    true,
						PlanModifiers: []planmodifier.Int64{
							int64planmodifier.RequiresReplace(),
						},
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
						ElementType: types.StringType,
						PlanModifiers: []planmodifier.Map{
							mapplanmodifier.RequiresReplace(),
						},
					},
					"all_configs": schema.MapAttribute{
						Description: "All topic configurations including any server set configurations",
						Computed:    true,
						ElementType: types.StringType,
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
						ElementType: types.StringType,
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
						ElementType: types.StringType,
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
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "internal error", fmt.Errorf("invalid provider data"))
		return
	}

	d.cfg = cfg
}

func (d *EntityResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_entity"
}

// Create implements resource.Resource.
func (d *EntityResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var entity EntityResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &entity)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	entityPath := []string{}
	resp.Diagnostics.Append(entity.EntityPath.ElementsAs(ctx, &entityPath, false)...)
	if resp.Diagnostics.HasError() {
		return
	}

	storeType, err := getStoreType(ctx, conn, entity.Store.ValueString())
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid store type", err)
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

		if !kafkaProperties.Configs.IsNull() {
			configProps := kafkaProperties.Configs.Elements()
			for k, v := range configProps {
				properties = append(properties, fmt.Sprintf("'kafka.topic.%s' = '%s'", k, v.(*types.String).ValueString()))
			}
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

	dsql, err := util.ExecTemplate(createEntityTmpl, map[string]any{
		"StoreName":  entity.Store.ValueString(),
		"EntityPath": entityPath,
		"Properties": strings.Join(properties, ", "),
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
	}
	if _, err := conn.ExecContext(ctx, dsql); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create entity", err)
		return
	}

	resp.Diagnostics.Append(d.updateComputed(ctx, &entity)...)
	if resp.Diagnostics.HasError() {
		return
	}

	tflog.Info(ctx, "Entity created", map[string]any{"store": entity.Store.String(), "name": entity.EntityPath.String()})
	resp.Diagnostics.Append(resp.State.Set(ctx, entity)...)
}

func (d *EntityResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var entity EntityResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &entity)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	entityPath := []string{}
	resp.Diagnostics.Append(entity.EntityPath.ElementsAs(ctx, &entityPath, false)...)
	if resp.Diagnostics.HasError() {
		return
	}

	dsql, err := util.ExecTemplate(dropEntityTmpl, map[string]any{
		"StoreName":  entity.Store.ValueString(),
		"EntityPath": entityPath,
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
	}
	if _, err := conn.ExecContext(ctx, dsql); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create database", err)
		return
	}
	tflog.Info(ctx, "Entity deleted", map[string]any{"store": entity.Store.String(), "name": entity.EntityPath.String()})
}

func (d *EntityResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "update not supported", fmt.Errorf("store entity update not supported"))
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

	dsql, err := util.ExecTemplate(describeEntityTmpl, map[string]any{
		"StoreName":  entity.Store.ValueString(),
		"EntityPath": entityPath,
	})
	if err != nil {
		diags = util.LogError(ctx, diags, "failed to generate SQL", err)
	}
	rows, err := conn.QueryContext(ctx, dsql)
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
		if err := rows.Scan(&discard, &discard, &topicPartitions, &topicReplicas, &keyDescriptor, &valueDescriptor, &configJSON); err != nil {
			diags.AddError("failed to read entity", err.Error())
			return
		}
		var kafkaProperties KafkaStoreEntityResourceData
		if !entity.KafkaProperties.IsNull() && !entity.KafkaProperties.IsUnknown() {
			diags.Append(entity.KafkaProperties.As(ctx, &kafkaProperties, basetypes.ObjectAsOptions{})...)
			if diags.HasError() {
				return
			}
		}
		kafkaProperties.TopicPartitions = types.Int64Value(topicPartitions)
		kafkaProperties.TopicReplicas = types.Int64Value(topicReplicas)
		kafkaProperties.KeyDescriptor = types.StringPointerValue(keyDescriptor)
		kafkaProperties.ValueDescriptor = types.StringPointerValue(valueDescriptor)
		if kafkaProperties.Configs.IsNull() || kafkaProperties.Configs.IsUnknown() {
			kafkaProperties.Configs = types.MapNull(types.StringType)
		}
		configsOut := map[string]string{}
		if err := json.Unmarshal([]byte(configJSON), &configsOut); err != nil {
			diags.AddError("failed to read entity configuration", err.Error())
			return
		}
		var d diag.Diagnostics
		kafkaProperties.AllConfigs, d = types.MapValueFrom(ctx, types.StringType, configsOut)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		entity.KafkaProperties, d = types.ObjectValueFrom(ctx, kafkaProperties.AttributeTypes(), kafkaProperties)
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
		if !entity.KinesisProperties.IsNull() && !entity.KinesisProperties.IsUnknown() {
			diags.Append(entity.KinesisProperties.As(ctx, &kinesisProperties, basetypes.ObjectAsOptions{})...)
			if diags.HasError() {
				return
			}
		}
		kinesisProperties.KinesisShards = types.Int64Value(topicShards)
		kinesisProperties.Descriptor = types.StringValue(descriptor)
		var d diag.Diagnostics
		entity.KinesisProperties, d = types.ObjectValueFrom(ctx, kinesisProperties.AttributeTypes(), kinesisProperties)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		entity.KinesisProperties, d = types.ObjectValueFrom(ctx, kinesisProperties.AttributeTypes(), kinesisProperties)
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
		if !entity.SnowflakeProperties.IsNull() && !entity.SnowflakeProperties.IsUnknown() {
			diags.Append(entity.SnowflakeProperties.As(ctx, &snowflakeProperties, basetypes.ObjectAsOptions{})...)
			if diags.HasError() {
				return
			}
		}
		var d diag.Diagnostics
		snowflakeProperties.Details, d = types.MapValueFrom(ctx, types.StringType, detail)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		entity.SnowflakeProperties, d = types.ObjectValueFrom(ctx, snowflakeProperties.AttributeTypes(), snowflakeProperties)
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
		if !entity.PostgresProperties.IsNull() && !entity.PostgresProperties.IsUnknown() {
			diags.Append(entity.PostgresProperties.As(ctx, &postgresProperties, basetypes.ObjectAsOptions{})...)
			if diags.HasError() {
				return
			}
		}
		var d diag.Diagnostics
		postgresProperties.Details, d = types.MapValueFrom(ctx, types.StringType, detail)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
		entity.SnowflakeProperties, d = types.ObjectValueFrom(ctx, postgresProperties.AttributeTypes(), postgresProperties)
		diags.Append(d...)
		if diags.HasError() {
			return
		}
	}

	if entity.KafkaProperties.IsUnknown() {
		entity.KafkaProperties = types.ObjectNull(KafkaStoreEntityResourceData{}.AttributeTypes())
	}
	if entity.KinesisProperties.IsUnknown() {
		entity.KinesisProperties = types.ObjectNull(KinesisStoreEntityResourceData{}.AttributeTypes())
	}
	if entity.SnowflakeProperties.IsUnknown() {
		entity.SnowflakeProperties = types.ObjectNull(SnowflakeStoreEntityResourceData{}.AttributeTypes())
	}
	if entity.PostgresProperties.IsUnknown() {
		entity.PostgresProperties = types.ObjectNull(PostgresStoreEntityResourceData{}.AttributeTypes())
	}

	return
}

func getStoreType(ctx context.Context, conn *sql.Conn, storeName string) (string, error) {
	dsql, err := util.ExecTemplate(lookupStoreTypeTmpl, map[string]any{
		"Name": storeName,
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate SQL: %w", err)
	}
	row := conn.QueryRowContext(ctx, dsql)
	if row.Err() != nil {
		if row.Err() == sql.ErrNoRows {
			return "", fmt.Errorf("store not found: %s", storeName)
		}

		return "", fmt.Errorf("failed to read store: %w", row.Err())
	}

	var kind string
	if err := row.Scan(&kind); err != nil {
		return "", fmt.Errorf("failed to read store: %w", row.Err())
	}

	return kind, nil
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
