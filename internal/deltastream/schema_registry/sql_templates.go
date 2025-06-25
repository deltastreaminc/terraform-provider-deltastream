package schemaregistry

import "github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

var lookupSchemaRegistryTmpl = util.ParseTemplate(`SELECT type, "status", "owner", created_at, updated_at FROM deltastream.sys."schema_registries" WHERE name = '{{.Name}}';`)
var listSchemaRegistryTmpl = util.ParseTemplate(`SELECT name, type, "status", "owner", created_at, updated_at FROM deltastream.sys."schema_registries";`)
var createSchemaRegistryTmpl = util.ParseTemplate(`CREATE SCHEMA_REGISTRY "{{EscapeIdentifier .Name}}" WITH(
	{{- if eq .Type "CONFLUENT" -}}
		'type' = CONFLUENT, 'uris' = '{{.Confluent.Uris.ValueString}}'
		{{- if and (not .Confluent.Username.IsNull) (not .Confluent.Username.IsUnknown) -}}
		,'confluent.username' = '{{.Confluent.Username.ValueString}}', 'confluent.password' = '{{.Confluent.Password.ValueString}}'
		{{- end -}}
	{{- end -}}
	{{- if eq .Type "CONFLUENT_CLOUD" -}}
		'type' = CONFLUENT_CLOUD, 'uris' = '{{.ConfluentCloud.Uris.ValueString}}',
		'confluent_cloud.key' = '{{.ConfluentCloud.Key.ValueString}}', 'confluent_cloud.secret' = '{{.ConfluentCloud.Secret.ValueString}}'
	{{- end -}}
);`)
var dropSchemaRegistryTmpl = util.ParseTemplate(`DROP SCHEMA_REGISTRY "{{EscapeIdentifier .Name}}";`)
