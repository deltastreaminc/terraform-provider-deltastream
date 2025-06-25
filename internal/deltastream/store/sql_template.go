package store

import "github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

var createStoreTmpl = util.ParseTemplate(`CREATE STORE "{{EscapeIdentifier .Name}}" WITH(
	{{- if eq .Type "KAFKA" }}
		'type' = KAFKA, 'kafka.sasl.hash_function' = {{.Kafka.SaslHashFunc.ValueString}},
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
		'type' = CONFLUENT_KAFKA,  'kafka.sasl.hash_function' = {{.ConfluentKafka.SaslHashFunc.ValueString}}, 'kafka.sasl.username' = '{{.ConfluentKafka.SaslUsername.ValueString}}', 'kafka.sasl.password' = '{{.ConfluentKafka.SaslPassword.ValueString}}',
		{{- if not (or .ConfluentKafka.SchemaRegistry.IsNull .ConfluentKafka.SchemaRegistry.IsUnknown) }}
			'schema_registry.name' = "{{.ConfluentKafka.SchemaRegistry.ValueString}}",
		{{- end }}
		'tls.verify_server_hostname' = TRUE,
		'uris' = '{{.ConfluentKafka.Uris.ValueString}}'
	{{- end }}
	{{- if eq .Type "KINESIS" }}
		'type' = KINESIS, 'kinesis.account.id' = '{{.Kinesis.AwsAccountId}}',
		{{- if and .Kinesis.AccessKeyId .Kinesis.SecretAccessKey }}
			'kinesis.access_key_id' = '{{.Kinesis.AccessKeyId.ValueString}}', 'kinesis.secret_access_key' = '{{.Kinesis.SecretAccessKey.ValueString}}',
		{{- end }}
		'uris' = '{{.Kinesis.Uris.ValueString}}'
	{{- end }}
	{{- if eq .Type "SNOWFLAKE" }}
		'type' = SNOWFLAKE, 'snowflake.account_id' = '{{.Snowflake.AccountId.ValueString}}', 'snowflake.cloud.region' = '{{.Snowflake.CloudRegion.ValueString}}', 'snowflake.warehouse_name' = '{{.Snowflake.WarehouseName.ValueString}}', 'snowflake.role_name' = '{{.Snowflake.RoleName.ValueString}}', 'snowflake.username' = '{{.Snowflake.Username.ValueString}}', 'snowflake.client.key_file' = 'snowflake.client.key_file.pem', 'snowflake.client.key_passphrase' = '{{.Snowflake.ClientKeyPassphrase.ValueString}}', 'uris' = '{{.Snowflake.Uris.ValueString}}'
	{{- end }}
	{{- if eq .Type "POSTGRESQL" }}
		'type' = POSTGRESQL, 'postgres.username' = '{{.Postgres.Username.ValueString}}', 'postgres.password' = '{{.Postgres.Password.ValueString}}', 'uris' = '{{.Postgres.Uris.ValueString}}'
	{{- end }}
);`)
var dropStoreTmpl = util.ParseTemplate(`DROP STORE "{{EscapeIdentifier .Name}}";`)
var lookupStoreTmpl = util.ParseTemplate(`SELECT type, status, "owner", created_at, updated_at FROM deltastream.sys."stores" WHERE name = '{{.Name}}';`)
var lookupStoreTypeTmpl = util.ParseTemplate(`SELECT type FROM deltastream.sys."stores" WHERE name = '{{.Name}}';`)
var describeStoreTmpl = util.ParseTemplate(`DESCRIBE STORE "{{EscapeIdentifier .Name}}";`)
var listStoresTmpl = util.ParseTemplate(`SELECT name, type, status, "owner", created_at, updated_at FROM deltastream.sys."stores";`)

var listEntityTmpl = util.ParseTemplate(`LIST ENTITIES 
	{{ if ne (len .ParentPath) 0 }}
	IN {{ range $index, $element := .ParentPath -}}
        {{- if $index}}.{{end -}}
        "{{EscapeIdentifier $element}}"
    {{- end }}
	{{ end }}
	IN STORE "{{ EscapeIdentifier .StoreName }}";
`)
var createEntityTmpl = util.ParseTemplate(`CREATE ENTITY {{ range $index, $element := .EntityPath -}}
	{{- if $index}}.{{end -}}
		"{{- EscapeIdentifier $element}}"
    {{- end }}
	IN STORE "{{ EscapeIdentifier .StoreName }}"
	{{ if .Properties }} WITH ( {{ .Properties }} ){{ end }}
;`)
var dropEntityTmpl = util.ParseTemplate(`DROP ENTITY 	
	{{ range $index, $element := .EntityPath }}
		{{ if $index }}.{{ end }}
		"{{- EscapeIdentifier $element }}"
	{{ end }}
	IN STORE "{{ EscapeIdentifier .StoreName }}"
;`)
var describeEntityTmpl = util.ParseTemplate(`DESCRIBE ENTITY 	
	{{ range $index, $element := .EntityPath }}
		{{ if $index }}.{{ end }}
		"{{- EscapeIdentifier $element }}"
	{{ end }}
	IN STORE "{{ EscapeIdentifier .StoreName }}"
;`)
var printEntityTmpl = util.ParseTemplate(`PRINT ENTITY
	{{ if ne (len .EntityPath) 0 }}
	{{- range $index, $element := .EntityPath -}}
        {{- if $index }}.{{ end -}}
    	"{{- EscapeIdentifier $element }}"
    {{- end }}
	{{- end }}
	IN STORE "{{ EscapeIdentifier .StoreName }}"
	{{ if .FromBeginning }}WITH ( 'from_beginning' ){{ end }};
`)
