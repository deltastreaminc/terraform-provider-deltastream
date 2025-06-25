// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-framework/types/basetypes"
	"github.com/hashicorp/terraform-plugin-log/tflog"
	"github.com/sethvargo/go-retry"

	gods "github.com/deltastreaminc/go-deltastream"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/provider/config"
	"github.com/deltastreaminc/terraform-provider-deltastream/internal/util"
)

var _ resource.Resource = &StoreResource{}
var _ resource.ResourceWithConfigure = &StoreResource{}

func NewStoreResource() resource.Resource {
	return &StoreResource{}
}

type StoreResource struct {
	cfg *config.DeltaStreamProviderCfg
}

type KafkaProperties struct {
	Uris                    types.String `tfsdk:"uris"`
	SchemaRegistry          types.String `tfsdk:"schema_registry_name"`
	SaslHashFunc            types.String `tfsdk:"sasl_hash_function"`
	SaslUsername            types.String `tfsdk:"sasl_username"`
	SaslPassword            types.String `tfsdk:"sasl_password"`
	MskIamRoleArn           types.String `tfsdk:"msk_iam_role_arn"`
	MskAwsRegion            types.String `tfsdk:"msk_aws_region"`
	TlsDisabled             types.Bool   `tfsdk:"tls_disabled"`
	TlsVerifyServerHostname types.Bool   `tfsdk:"tls_verify_server_hostname"`
	TlsCaCertFile           types.String `tfsdk:"tls_ca_cert_file"`
}

func (KafkaProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":                       types.StringType,
		"schema_registry_name":       types.StringType,
		"sasl_hash_function":         types.StringType,
		"sasl_username":              types.StringType,
		"sasl_password":              types.StringType,
		"msk_iam_role_arn":           types.StringType,
		"msk_aws_region":             types.StringType,
		"tls_disabled":               types.BoolType,
		"tls_verify_server_hostname": types.BoolType,
		"tls_ca_cert_file":           types.StringType,
	}
}

type ConfleuntKafkaProperties struct {
	Uris           types.String `tfsdk:"uris"`
	SchemaRegistry types.String `tfsdk:"schema_registry_name"`
	SaslHashFunc   types.String `tfsdk:"sasl_hash_function"`
	SaslUsername   types.String `tfsdk:"sasl_username"`
	SaslPassword   types.String `tfsdk:"sasl_password"`
}

type KinesisProperties struct {
	Uris            types.String `tfsdk:"uris"`
	AccessKeyId     types.String `tfsdk:"access_key_id"`
	SecretAccessKey types.String `tfsdk:"secret_access_key"`
	AwsAccountId    types.String `tfsdk:"aws_account_id"`
}

type SnowflakeProperties struct {
	Uris                types.String `tfsdk:"uris"`
	AccountId           types.String `tfsdk:"account_id"`
	CloudRegion         types.String `tfsdk:"cloud_region"`
	WarehouseName       types.String `tfsdk:"warehouse_name"`
	RoleName            types.String `tfsdk:"role_name"`
	Username            types.String `tfsdk:"username"`
	ClientKeyFile       types.String `tfsdk:"client_key_file"`
	ClientKeyPassphrase types.String `tfsdk:"client_key_passphrase"`
}

type PostgresProperties struct {
	Uris     types.String `tfsdk:"uris"`
	Username types.String `tfsdk:"username"`
	Password types.String `tfsdk:"password"`
}

type StoreResourceData struct {
	Name           types.String `tfsdk:"name"`
	Type           types.String `tfsdk:"type"`
	Kafka          types.Object `tfsdk:"kafka"`
	ConfleuntKafka types.Object `tfsdk:"confluent_kafka"`
	Kinesis        types.Object `tfsdk:"kinesis"`
	Snowflake      types.Object `tfsdk:"snowflake"`
	Postgres       types.Object `tfsdk:"postgres"`
	Owner          types.String `tfsdk:"owner"`
	State          types.String `tfsdk:"state"`
	UpdatedAt      types.String `tfsdk:"updated_at"`
	CreatedAt      types.String `tfsdk:"created_at"`
}

func (d *StoreResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Store resource",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the Store",
				Required:    true,
			},
			"type": schema.StringAttribute{
				Description: "Type of the Store",
				Computed:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},

			"kafka": schema.SingleNestedAttribute{
				Description: "Kafka specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Required:    true,
					},
					"schema_registry_name": schema.StringAttribute{
						Description: "Name of the schema registry",
						Optional:    true,
					},
					"sasl_hash_function": schema.StringAttribute{
						Description: "SASL hash function to use when authenticating with Apache Kafka brokers",
						Validators:  []validator.String{stringvalidator.OneOf("NONE", "AWS_MSK_IAM", "PLAIN", "SHA256", "SHA512")},
						Required:    true,
					},
					"sasl_username": schema.StringAttribute{
						Description: "Username to use when authenticating with Apache Kafka brokers",
						Optional:    true,
						Sensitive:   true,
					},
					"sasl_password": schema.StringAttribute{
						Description: "Password to use when authenticating with Apache Kafka brokers",
						Optional:    true,
						Sensitive:   true,
					},
					"msk_iam_role_arn": schema.StringAttribute{
						Description: "IAM role ARN to use when authenticating with Amazon MSK",
						Optional:    true,
						Sensitive:   true,
					},
					"msk_aws_region": schema.StringAttribute{
						Description: "AWS region where the Amazon MSK cluster is located",
						Optional:    true,
						Sensitive:   true,
					},
					"tls_disabled": schema.BoolAttribute{
						Description: "Specifies if the store should be accessed over TLS",
						Optional:    true,
						Computed:    true,
					},
					"tls_verify_server_hostname": schema.BoolAttribute{
						Description: "Specifies if the server CNAME should be validated against the certificate",
						Optional:    true,
						Computed:    true,
					},
					"tls_ca_cert_file": schema.StringAttribute{
						Description: "CA certificate in PEM format",
						Optional:    true,
					},
				},
				Optional: true,
			},

			"confluent_kafka": schema.SingleNestedAttribute{
				Description: "Confluent Kafka specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Required:    true,
					},
					"sasl_hash_function": schema.StringAttribute{
						Description: "SASL hash function to use when authenticating with Confluent Kafka brokers",
						Validators:  []validator.String{stringvalidator.OneOf("PLAIN", "SHA256", "SHA512")},
						Required:    true,
					},
					"sasl_username": schema.StringAttribute{
						Description: "Username to use when authenticating with Apache Kafka brokers",
						Required:    true,
						Sensitive:   true,
					},
					"sasl_password": schema.StringAttribute{
						Description: "Password to use when authenticating with Apache Kafka brokers",
						Required:    true,
						Sensitive:   true,
					},
				},
				Optional: true,
			},

			"kinesis": schema.SingleNestedAttribute{
				Description: "Kinesis specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Required:    true,
					},
					"access_key_id": schema.StringAttribute{
						Description: "AWS IAM access key to use when authenticating with an Amazon Kinesis service",
						Optional:    true,
						Sensitive:   true,
					},
					"secret_access_key": schema.StringAttribute{
						Description: "AWS IAM secret access key to use when authenticating with an Amazon Kinesis service",
						Optional:    true,
						Sensitive:   true,
					},
					"aws_account_id": schema.StringAttribute{
						Description: "AWS account ID to use when authenticating with an Amazon Kinesis service",
						Required:    true,
						Sensitive:   true,
					},
				},
				Optional: true,
			},

			"snowflake": schema.SingleNestedAttribute{
				Description: "Snowflake specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Required:    true,
					},
					"account_id": schema.StringAttribute{
						Description: "Snowflake account ID",
						Required:    true,
					},
					"cloud_region": schema.StringAttribute{
						Description: "Snowflake cloud region name, where the account resources operate in",
						Required:    true,
					},
					"warehouse_name": schema.StringAttribute{
						Description: "Warehouse name to use for queries and other store operations that require compute resource",
						Required:    true,
					},
					"role_name": schema.StringAttribute{
						Description: "Access control role to use for the Store operations after connecting to Snowflake",
						Required:    true,
					},
					"username": schema.StringAttribute{
						Description: "User login name for the Snowflake account",
						Required:    true,
						Sensitive:   true,
					},
					"client_key_file": schema.StringAttribute{
						Description: "Snowflake account's private key in PEM format",
						Required:    true,
						Sensitive:   true,
					},
					"client_key_passphrase": schema.StringAttribute{
						Description: "Passphrase for decrypting the Snowflake account's private key",
						Required:    true,
						Sensitive:   true,
					},
				},
				Optional: true,
			},

			"postgres": schema.SingleNestedAttribute{
				Description: "Postgres specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Required:    true,
					},
					"username": schema.StringAttribute{
						Description: "Username to use when authenticating with a Postgres database",
						Required:    true,
						Sensitive:   true,
					},
					"password": schema.StringAttribute{
						Description: "Password to use when authenticating with a Postgres database",
						Required:    true,
						Sensitive:   true,
					},
				},
				Optional: true,
			},

			"owner": schema.StringAttribute{
				Description: "Owning role of the Store",
				Optional:    true,
				Computed:    true,
			},
			"state": schema.StringAttribute{
				Description: "State of the Store",
				Computed:    true,
			},
			"created_at": schema.StringAttribute{
				Description: "Creation date of the Store",
				Computed:    true,
			},
			"updated_at": schema.StringAttribute{
				Description: "Last update date of the Store",
				Computed:    true,
			},
		},
	}
}

func (d *StoreResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (d *StoreResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_store"
}

// Create implements resource.Resource.
func (d *StoreResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var store StoreResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !store.Owner.IsNull() && !store.Owner.IsUnknown() {
		roleName = store.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	var kafkaProperties KafkaProperties
	var confluentKafkaProperties ConfleuntKafkaProperties
	var kinesisProperties KinesisProperties
	var snowflakeProperties SnowflakeProperties
	var postgresProperties PostgresProperties
	var stype string

	switch {
	case !store.Kafka.IsNull() && !store.Kafka.IsUnknown():
		stype = "KAFKA"
		resp.Diagnostics.Append(store.Kafka.As(ctx, &kafkaProperties, basetypes.ObjectAsOptions{})...)
		if kafkaProperties.TlsDisabled.IsNull() || kafkaProperties.TlsDisabled.IsUnknown() {
			kafkaProperties.TlsDisabled = types.BoolValue(false)
		}
		if kafkaProperties.TlsVerifyServerHostname.IsNull() || kafkaProperties.TlsVerifyServerHostname.IsUnknown() {
			kafkaProperties.TlsVerifyServerHostname = types.BoolValue(true)
		}
		var dg diag.Diagnostics
		store.Kafka, dg = types.ObjectValueFrom(ctx, kafkaProperties.AttributeTypes(), kafkaProperties)
		resp.Diagnostics.Append(dg...)
	case !store.ConfleuntKafka.IsNull() && !store.ConfleuntKafka.IsUnknown():
		stype = "CONFLUENT_KAFKA"
		resp.Diagnostics.Append(store.ConfleuntKafka.As(ctx, &confluentKafkaProperties, basetypes.ObjectAsOptions{})...)
	case !store.Kinesis.IsNull() && !store.Kinesis.IsUnknown():
		stype = "KINESIS"
		resp.Diagnostics.Append(store.Kinesis.As(ctx, &kinesisProperties, basetypes.ObjectAsOptions{})...)
	case !store.Snowflake.IsNull() && !store.Snowflake.IsUnknown():
		stype = "SNOWFLAKE"
		resp.Diagnostics.Append(store.Snowflake.As(ctx, &snowflakeProperties, basetypes.ObjectAsOptions{})...)
		b := io.NopCloser(bytes.NewBuffer([]byte(snowflakeProperties.ClientKeyFile.ValueString())))
		ctx = gods.WithAttachment(ctx, "snowflake.client.key_file.pem", b)
	case !store.Postgres.IsNull() && !store.Postgres.IsUnknown():
		stype = "POSTGRESQL"
		resp.Diagnostics.Append(store.Postgres.As(ctx, &postgresProperties, basetypes.ObjectAsOptions{})...)
	default:
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid store", fmt.Errorf("must specify atleast one store type properties"))
	}
	if resp.Diagnostics.HasError() {
		return
	}

	dsql, err := util.ExecTemplate(createStoreTmpl, map[string]any{
		"Name":           store.Name.ValueString(),
		"Type":           stype,
		"Kafka":          kafkaProperties,
		"ConfluentKafka": confluentKafkaProperties,
		"Kinesis":        kinesisProperties,
		"Snowflake":      snowflakeProperties,
		"Postgres":       postgresProperties,
	})
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", err)
		return
	}
	if _, err := conn.ExecContext(ctx, dsql); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create store", err)
		return
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		store, err = d.updateComputed(ctx, conn, store)
		if err != nil {
			return err
		}

		if store.State.ValueString() != "ready" {
			return retry.RetryableError(errors.New("store not ready"))
		}
		return nil
	}); err != nil {
		dsql, derr := util.ExecTemplate(dropStoreTmpl, map[string]any{
			"Name": store.Name.ValueString(),
		})
		if derr != nil {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to generate SQL", derr)
			return
		}
		if _, derr := conn.ExecContext(ctx, dsql); derr != nil {
			var sqlErr gods.ErrSQLError
			if !(errors.As(derr, &sqlErr) && sqlErr.SQLCode != gods.SqlStateInvalidParameter) {
				tflog.Error(ctx, "failed to clean up store", map[string]any{
					"name":  store.Name.ValueString(),
					"error": derr.Error(),
				})
			}
		}

		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to create store", err)
		return
	}
	tflog.Info(ctx, "Store created", map[string]any{"name": store.Name.ValueString()})
	resp.Diagnostics.Append(resp.State.Set(ctx, store)...)
}

func (d *StoreResource) updateComputed(ctx context.Context, conn *sql.Conn, store StoreResourceData) (StoreResourceData, error) {
	dsql, err := util.ExecTemplate(lookupStoreTmpl, map[string]any{
		"Name": store.Name.ValueString(),
	})
	if err != nil {
		return store, fmt.Errorf("failed to generate SQL: %w", err)
	}
	row := conn.QueryRowContext(ctx, dsql)
	if row.Err() != nil {
		if errors.Is(row.Err(), sql.ErrNoRows) {
			return store, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidStore}
		}

		return store, row.Err()
	}

	var kind string
	var state string
	var owner string
	var createdAt time.Time
	var updatedAt time.Time
	if err := row.Scan(&kind, &state, &owner, &createdAt, &updatedAt); err != nil {
		return store, err
	}

	store.Type = types.StringValue(kind)
	store.State = types.StringValue(state)
	store.Owner = types.StringValue(owner)
	store.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	store.UpdatedAt = types.StringValue(updatedAt.Format(time.RFC3339))
	store.Owner = types.StringValue(owner)
	store.CreatedAt = types.StringValue(createdAt.Format(time.RFC3339))
	return store, nil
}

func (d *StoreResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var store StoreResourceData

	resp.Diagnostics.Append(req.State.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !store.Owner.IsNull() && !store.Owner.IsUnknown() {
		roleName = store.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) error {
		dsql, err := util.ExecTemplate(dropStoreTmpl, map[string]any{
			"Name": store.Name.ValueString(),
		})
		if err != nil {
			return fmt.Errorf("failed to generate SQL: %w", err)
		}
		if _, err := conn.ExecContext(ctx, dsql); err != nil {
			var sqlErr gods.ErrSQLError
			if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidStore {
				return retry.RetryableError(err)
			}
		}
		return nil
	}); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to drop store", err)
		return
	}

	if err := retry.Do(ctx, retry.WithMaxDuration(time.Minute*5, retry.NewExponential(time.Second)), func(ctx context.Context) (err error) {
		store, err = d.updateComputed(ctx, conn, store)
		if err != nil {
			var godsErr gods.ErrSQLError
			if errors.As(err, &godsErr) && godsErr.SQLCode != gods.SqlStateInvalidStore {
				return retry.RetryableError(err)
			}
			return nil
		}
		return retry.RetryableError(fmt.Errorf("timedout waiting for store to be deleted"))
	}); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to delete store", err)
	}

	tflog.Info(ctx, "Store deleted", map[string]any{"name": store.Name.ValueString()})
}

func (d *StoreResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "update not supported", fmt.Errorf("store update not supported"))
}

func (d *StoreResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var store StoreResourceData

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !store.Owner.IsNull() && !store.Owner.IsUnknown() {
		roleName = store.Owner.ValueString()
	}

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, roleName)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	store, err = d.updateComputed(ctx, conn, store)
	if err != nil {
		var godsErr gods.ErrSQLError
		if errors.As(err, &godsErr) && godsErr.SQLCode == gods.SqlStateInvalidStore {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, store)...)
}
