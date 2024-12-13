package schema

import "github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

var lookupSchemaTmpl = util.ParseTemplate(`SELECT "owner", created_at FROM deltastream.sys."schemas" WHERE database_name = '{{.DatabaseName}}' AND name = '{{.Name}}';`)
var listSchemasTmpl = util.ParseTemplate(`SELECT name, "owner", created_at FROM deltastream.sys."schemas" WHERE database_name = '{{.DatabaseName}}';`)
var createSchemaTmpl = util.ParseTemplate(`CREATE SCHEMA "{{EscapeIdentifier .Name}}" IN DATABASE "{{EscapeIdentifier .DatabaseName}}";`)
var dropSchemaTmpl = util.ParseTemplate(`DROP SCHEMA "{{EscapeIdentifier .DatabaseName}}"."{{EscapeIdentifier .Name}}";`)
