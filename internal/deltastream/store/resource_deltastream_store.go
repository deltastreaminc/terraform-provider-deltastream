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
	"text/template"
	"time"

	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/attr"
	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
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
	Uris                    basetypes.StringValue `tfsdk:"uris"`
	SchemaRegistry          basetypes.StringValue `tfsdk:"schema_registry_name"`
	SaslHashFunc            basetypes.StringValue `tfsdk:"sasl_hash_function"`
	SaslUsername            basetypes.StringValue `tfsdk:"sasl_username"`
	SaslPassword            basetypes.StringValue `tfsdk:"sasl_password"`
	MskIamRoleArn           basetypes.StringValue `tfsdk:"msk_iam_role_arn"`
	MskAwsRegion            basetypes.StringValue `tfsdk:"msk_aws_region"`
	TlsDisabled             basetypes.BoolValue   `tfsdk:"tls_disabled"`
	TlsVerifyServerHostname basetypes.BoolValue   `tfsdk:"tls_verify_server_hostname"`
	TlsCaCertFile           basetypes.StringValue `tfsdk:"tls_ca_cert_file"`
}

func (_ KafkaProperties) AttributeTypes() map[string]attr.Type {
	return map[string]attr.Type{
		"uris":                       basetypes.StringType{},
		"schema_registry_name":       basetypes.StringType{},
		"sasl_hash_function":         basetypes.StringType{},
		"sasl_username":              basetypes.StringType{},
		"sasl_password":              basetypes.StringType{},
		"msk_iam_role_arn":           basetypes.StringType{},
		"msk_aws_region":             basetypes.StringType{},
		"tls_disabled":               basetypes.BoolType{},
		"tls_verify_server_hostname": basetypes.BoolType{},
		"tls_ca_cert_file":           basetypes.StringType{},
	}
}

type ConfleuntKafkaProperties struct {
	Uris           basetypes.StringValue `tfsdk:"uris"`
	SchemaRegistry basetypes.StringValue `tfsdk:"schema_registry_name"`
	SaslHashFunc   basetypes.StringValue `tfsdk:"sasl_hash_function"`
	SaslUsername   basetypes.StringValue `tfsdk:"sasl_username"`
	SaslPassword   basetypes.StringValue `tfsdk:"sasl_password"`
}

type KinesisProperties struct {
	Uris            basetypes.StringValue `tfsdk:"uris"`
	SchemaRegistry  basetypes.StringValue `tfsdk:"schema_registry_name"`
	AccessKeyId     basetypes.StringValue `tfsdk:"access_key_id"`
	SecretAccessKey basetypes.StringValue `tfsdk:"secret_access_key"`
}

type SnowflakeProperties struct {
	Uris                basetypes.StringValue `tfsdk:"uris"`
	AccountId           basetypes.StringValue `tfsdk:"account_id"`
	CloudRegion         basetypes.StringValue `tfsdk:"cloud_region"`
	WarehouseName       basetypes.StringValue `tfsdk:"warehouse_name"`
	RoleName            basetypes.StringValue `tfsdk:"role_name"`
	Username            basetypes.StringValue `tfsdk:"username"`
	ClientKeyFile       basetypes.StringValue `tfsdk:"client_key_file"`
	ClientKeyPassphrase basetypes.StringValue `tfsdk:"client_key_passphrase"`
}

type DatabricksProperties struct {
	Uris            basetypes.StringValue `tfsdk:"uris"`
	AppToken        basetypes.StringValue `tfsdk:"app_token"`
	WarehouseId     basetypes.StringValue `tfsdk:"warehouse_id"`
	AccessKeyId     basetypes.StringValue `tfsdk:"access_key_id"`
	SecretAccessKey basetypes.StringValue `tfsdk:"secret_access_key"`
	CloudS3Bucket   basetypes.StringValue `tfsdk:"cloud_s3_bucket"`
	CloudRegion     basetypes.StringValue `tfsdk:"cloud_region"`
}

type PostgresProperties struct {
	Uris     basetypes.StringValue `tfsdk:"uris"`
	Username basetypes.StringValue `tfsdk:"username"`
	Password basetypes.StringValue `tfsdk:"password"`
}

type StoreResourceData struct {
	Name           basetypes.StringValue `tfsdk:"name"`
	AccessRegion   basetypes.StringValue `tfsdk:"access_region"`
	Type           basetypes.StringValue `tfsdk:"type"`
	Kafka          basetypes.ObjectValue `tfsdk:"kafka"`
	ConfleuntKafka basetypes.ObjectValue `tfsdk:"confluent_kafka"`
	Kinesis        basetypes.ObjectValue `tfsdk:"kinesis"`
	Snowflake      basetypes.ObjectValue `tfsdk:"snowflake"`
	Databricks     basetypes.ObjectValue `tfsdk:"databricks"`
	Postgres       basetypes.ObjectValue `tfsdk:"postgres"`
	Owner          basetypes.StringValue `tfsdk:"owner"`
	State          basetypes.StringValue `tfsdk:"state"`
	UpdatedAt      basetypes.StringValue `tfsdk:"updated_at"`
	CreatedAt      basetypes.StringValue `tfsdk:"created_at"`
}

func (d *StoreResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Store resource",
		Attributes: map[string]schema.Attribute{
			"name": schema.StringAttribute{
				Description: "Name of the Store",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"access_region": schema.StringAttribute{
				Description: "Specifies the region of the Store. In order to improve latency and reduce data transfer costs, the region should be the same cloud and region that the physical Store is running in.",
				Required:    true,
				Validators:  util.IdentifierValidators,
			},
			"type": schema.StringAttribute{
				Description: "Type of the Store",
				Computed:    true,
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
					"schema_registry_name": schema.StringAttribute{
						Description: "Name of the schema registry",
						Optional:    true,
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
					"schema_registry_name": schema.StringAttribute{
						Description: "Name of the schema registry",
						Optional:    true,
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

			"databricks": schema.SingleNestedAttribute{
				Description: "Databricks specific configuration",
				Attributes: map[string]schema.Attribute{
					"uris": schema.StringAttribute{
						Description: "List of host:port URIs to connect to the store",
						Required:    true,
					},
					"app_token": schema.StringAttribute{
						Description: "Databricks personal access token used when authenticating with a Databricks workspace",
						Required:    true,
					},
					"warehouse_id": schema.StringAttribute{
						Description: "The identifier for a Databricks SQL Warehouse belonging to a Databricks workspace. This Warehouse will be used to create and query Tables in Databricks",
						Required:    true,
					},
					"access_key_id": schema.StringAttribute{
						Description: "AWS access key ID used for writing data to S3",
						Required:    true,
						Sensitive:   true,
					},
					"secret_access_key": schema.StringAttribute{
						Description: "AWS secret access key used for writing data to S3",
						Required:    true,
						Sensitive:   true,
					},
					"cloud_s3_bucket": schema.StringAttribute{
						Description: "The name of the S3 bucket where the data will be stored",
						Required:    true,
					},
					"cloud_region": schema.StringAttribute{
						Description: "The region where the S3 bucket is located",
						Required:    true,
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
				Validators:  util.IdentifierValidators,
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

const createStatement = `CREATE STORE "{{.Name}}" WITH(
	{{- if eq .Type "KAFKA" }}
		'type' = KAFKA, 'access_region' = "{{.AccessRegion}}", 'kafka.sasl.hash_function' = {{.Kafka.SaslHashFunc.ValueString}},
		{{- if eq .Kafka.SaslHashFunc.ValueString "AWS_MSK_IAM" }}
			'kafka.msk.iam_role_arn' = '{{.Kafka.MskIamRoleArn.ValueString}}', 'kafka.msk.aws_region' = '{{.Kafka.MskAwsRegion.ValueString}}',
		{{- else if ne .Kafka.SaslHashFunc.ValueString "NONE" }}
			'kafka.sasl.username' = '{{.Kafka.SaslUsername.ValueString}}', 'kafka.sasl.password' = '{{.Kafka.SaslPassword.ValueString}}',
		{{- end }}
		{{- if not (or .Kafka.SchemaRegistry.IsNull .Kafka.SchemaRegistry.IsUnknown) }}
			'schema_registry.name' = "{{.Kafka.SchemaRegistry.ValueString}}",
		{{- end }}
		'tls.disabled' = {{ if .Kafka.TlsDisabled.ValueBool }}TRUE{{ else }}FALSE{{ end }},
		'tls.verify_server_hostname' = {{ if .Kafka.TlsVerifyServerHostname.ValueBool }}TRUE{{ else }}FALSE{{ end }},
		{{- if not (or .Kafka.TlsCaCertFile.IsNull .Kafka.TlsCaCertFile.IsUnknown) }}
			'tls.ca_cert_file' = 'tls.ca_cert_file.pem',
		{{- end }}
		'uris' = '{{.Kafka.Uris.ValueString}}'
	{{- end }}
	{{- if eq .Type "CONFLUENT_KAFKA" }}
		'type' = CONFLUENT_KAFKA, 'access_region' = "{{.AccessRegion}}", 'kafka.sasl.hash_function' = {{.ConfluentKafka.SaslHashFunc.ValueString}}, 'kafka.sasl.username' = '{{.ConfluentKafka.SaslUsername.ValueString}}', 'kafka.sasl.password' = '{{.ConfluentKafka.SaslPassword.ValueString}}',
		{{- if not (or .ConfluentKafka.SchemaRegistry.IsNull .ConfluentKafka.SchemaRegistry.IsUnknown) }}
			'schema_registry.name' = "{{.ConfluentKafka.SchemaRegistry.ValueString}}",
		{{- end }}
		'tls.verify_server_hostname' = TRUE,
		'uris' = '{{.ConfluentKafka.Uris.ValueString}}'
	{{- end }}
	{{- if eq .Type "KINESIS" }}
		'type' = KINESIS, 'access_region' = "{{.AccessRegion}}",
		{{- if and .Kinesis.AccessKeyId .Kinesis.SecretAccessKey }}
			'kinesis.access_key_id' = '{{.Kinesis.AccessKeyId.ValueString}}', 'kinesis.secret_access_key' = '{{.Kinesis.SecretAccessKey.ValueString}}',
		{{- end }}
		{{- if not (or .Kinesis.SchemaRegistry.IsNull .Kinesis.SchemaRegistry.IsUnknown) }}
			'schema_registry.name' = "{{.Kinesis.SchemaRegistry.ValueString}}",
		{{- end }}
		'uris' = '{{.Kinesis.Uris.ValueString}}'
	{{- end }}
	{{- if eq .Type "SNOWFLAKE" }}
		'type' = SNOWFLAKE, 'access_region' = "{{.AccessRegion}}", 'snowflake.account_id' = '{{.Snowflake.AccountId.ValueString}}', 'snowflake.cloud.region' = '{{.Snowflake.CloudRegion.ValueString}}', 'snowflake.warehouse_name' = '{{.Snowflake.WarehouseName.ValueString}}', 'snowflake.role_name' = '{{.Snowflake.RoleName.ValueString}}', 'snowflake.username' = '{{.Snowflake.Username.ValueString}}', 'snowflake.client.key_file' = 'snowflake.client.key_file.pem', 'snowflake.client.key_passphrase' = '{{.Snowflake.ClientKeyPassphrase.ValueString}}', 'uris' = '{{.Snowflake.Uris.ValueString}}'
	{{- end }}
	{{- if eq .Type "DATABRICKS" }}
		'type' = DATABRICKS, 'access_region' = "{{.AccessRegion}}", 'databricks.app_token' = '{{.Databricks.AppToken.ValueString}}', 'databricks.warehouse_id' = '{{.Databricks.WarehouseId.ValueString}}', 'databricks.warehouse_port' = 443, 'aws.access_key_id' = '{{.Databricks.AccessKeyId.ValueString}}', 'aws.secret_access_key' = '{{.Databricks.SecretAccessKey.ValueString}}', 'databricks.cloud.s3.bucket' = '{{.Databricks.CloudS3Bucket.ValueString}}', 'databricks.cloud.region' = '{{.Databricks.CloudRegion.ValueString}}', 'uris' = '{{.Databricks.Uris.ValueString}}'
	{{- end }}
	{{- if eq .Type "POSTGRESQL" }}
		'type' = POSTGRESQL, 'access_region' = "{{.AccessRegion}}", 'postgres.username' = '{{.Postgres.Username.ValueString}}', 'postgres.password' = '{{.Postgres.Password.ValueString}}', 'uris' = '{{.Postgres.Uris.ValueString}}'
	{{- end }}
);`

// Create implements resource.Resource.
func (d *StoreResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var store StoreResourceData

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !store.Owner.IsNull() && !store.Owner.IsUnknown() {
		roleName = store.Owner.ValueString()
	}

	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	var kafkaProperties KafkaProperties
	var confluentKafkaProperties ConfleuntKafkaProperties
	var kinesisProperties KinesisProperties
	var snowflakeProperties SnowflakeProperties
	var databricksProperties DatabricksProperties
	var postgresProperties PostgresProperties
	var stype string

	switch {
	case !store.Kafka.IsNull() && !store.Kafka.IsUnknown():
		stype = "KAFKA"
		resp.Diagnostics.Append(store.Kafka.As(ctx, &kafkaProperties, basetypes.ObjectAsOptions{})...)
		if kafkaProperties.TlsDisabled.IsNull() || kafkaProperties.TlsDisabled.IsUnknown() {
			kafkaProperties.TlsDisabled = basetypes.NewBoolValue(false)
		}
		if kafkaProperties.TlsVerifyServerHostname.IsNull() || kafkaProperties.TlsVerifyServerHostname.IsUnknown() {
			kafkaProperties.TlsVerifyServerHostname = basetypes.NewBoolValue(true)
		}
		var dg diag.Diagnostics
		store.Kafka, dg = basetypes.NewObjectValueFrom(ctx, kafkaProperties.AttributeTypes(), kafkaProperties)
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
	case !store.Databricks.IsNull() && !store.Databricks.IsUnknown():
		stype = "DATABRICKS"
		resp.Diagnostics.Append(store.Databricks.As(ctx, &databricksProperties, basetypes.ObjectAsOptions{})...)
	case !store.Postgres.IsNull() && !store.Postgres.IsUnknown():
		stype = "POSTGRESQL"
		resp.Diagnostics.Append(store.Postgres.As(ctx, &postgresProperties, basetypes.ObjectAsOptions{})...)
	default:
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid store", fmt.Errorf("must specify atleast one store type properties"))
	}
	if resp.Diagnostics.HasError() {
		return
	}

	b := bytes.NewBuffer(nil)
	if err := template.Must(template.New("").Parse(createStatement)).Execute(b, map[string]any{
		"Name":           store.Name.ValueString(),
		"Type":           stype,
		"AccessRegion":   store.AccessRegion.ValueString(),
		"Kafka":          kafkaProperties,
		"ConfluentKafka": confluentKafkaProperties,
		"Kinesis":        kinesisProperties,
		"Snowflake":      snowflakeProperties,
		"Databricks":     databricksProperties,
		"Postgres":       postgresProperties,
	}); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to render store sql", err)
		return
	}
	dsql := b.String()
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
		if _, derr := conn.ExecContext(ctx, `DROP STORE "`+store.Name.ValueString()+`";`); derr != nil {
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
	rows, err := conn.QueryContext(ctx, `LIST STORES;`)
	if err != nil {
		return store, err
	}
	defer rows.Close()

	for rows.Next() {
		var discard any
		var name string
		var accessRegion string
		var kind string
		var state string
		var owner string
		var createdAt time.Time
		var updatedAt time.Time
		if err := rows.Scan(&name, &kind, &accessRegion, &state, &discard, &owner, &createdAt, &updatedAt); err != nil {
			return store, err
		}
		if name == store.Name.ValueString() {
			store.Type = basetypes.NewStringValue(kind)
			store.AccessRegion = basetypes.NewStringValue(accessRegion)
			store.State = basetypes.NewStringValue(state)
			store.Owner = basetypes.NewStringValue(owner)
			store.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			store.UpdatedAt = basetypes.NewStringValue(updatedAt.Format(time.RFC3339))
			store.Owner = basetypes.NewStringValue(owner)
			store.CreatedAt = basetypes.NewStringValue(createdAt.Format(time.RFC3339))
			return store, nil
		}
	}
	return StoreResourceData{}, &gods.ErrSQLError{SQLCode: gods.SqlStateInvalidStore}
}

func (d *StoreResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var store StoreResourceData

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	resp.Diagnostics.Append(req.State.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	roleName := d.cfg.Role
	if !store.Owner.IsNull() && !store.Owner.IsUnknown() {
		roleName = store.Owner.ValueString()
	}
	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DROP STORE "%s";`, store.Name.ValueString())); err != nil {
		var sqlErr gods.ErrSQLError
		if !errors.As(err, &sqlErr) || sqlErr.SQLCode != gods.SqlStateInvalidStore {
			resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to drop store", err)
			return
		}
	}

	for {
		store, err = d.updateComputed(ctx, conn, store)
		if err != nil {
			var godsErr gods.ErrSQLError
			if errors.As(err, &godsErr) && godsErr.SQLCode != gods.SqlStateInvalidStore {
				resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
				return
			}
			break
		}
	}

	tflog.Info(ctx, "Store deleted", map[string]any{"name": store.Name.ValueString()})
}

func (d *StoreResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var currentStore StoreResourceData
	var newStore StoreResourceData

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &newStore)...)
	if resp.Diagnostics.HasError() {
		return
	}
	resp.Diagnostics.Append(req.State.Get(ctx, &currentStore)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// all changes to store other than ownership are disallowed
	if !newStore.Name.Equal(currentStore.Name) {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "invalid update", fmt.Errorf("store name cannot be changed"))
	}

	if !newStore.Owner.IsNull() && !newStore.Owner.IsUnknown() && newStore.Owner.Equal(currentStore.Owner) {
		// Transfer ownership
		tflog.Error(ctx, "transfer ownership not yet supported")
	}

	currentStore, err = d.updateComputed(ctx, conn, currentStore)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, currentStore)...)
}

func (d *StoreResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var store StoreResourceData

	ctx, conn, err := util.GetConnection(ctx, d.cfg.Db, d.cfg.SessionID, d.cfg.Organization, d.cfg.Role)
	if err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to connect", err)
		return
	}
	defer conn.Close()

	roleName := d.cfg.Role
	if !store.Owner.IsNull() && !store.Owner.IsUnknown() {
		roleName = store.Owner.ValueString()
	}

	if err := util.SetSqlContext(ctx, conn, &roleName, nil, nil, nil); err != nil {
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to set sql context", err)
		return
	}

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &store)...)
	if resp.Diagnostics.HasError() {
		return
	}

	store, err = d.updateComputed(ctx, conn, store)
	if err != nil {
		var godsErr gods.ErrSQLError
		if errors.As(err, &godsErr) && godsErr.SQLCode != gods.SqlStateInvalidStore {
			return
		}
		resp.Diagnostics = util.LogError(ctx, resp.Diagnostics, "failed to update state", err)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, store)...)
}
