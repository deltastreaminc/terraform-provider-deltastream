package namespace

import "github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

var lookupNamespaceTmpl = util.ParseTemplate(`SELECT "owner", created_at FROM deltastream.sys."schemas" WHERE database_name = '{{.DatabaseName}}' AND name = '{{.Name}}';`)
var listNamespacesTmpl = util.ParseTemplate(`SELECT name, "owner", created_at FROM deltastream.sys."schemas" WHERE database_name = '{{.DatabaseName}}';`)
var createNamespaceTmpl = util.ParseTemplate(`CREATE SCHEMA "{{EscapeIdentifier .Name}}" IN DATABASE "{{EscapeIdentifier .DatabaseName}}";`)
var dropNamespaceTmpl = util.ParseTemplate(`DROP SCHEMA "{{EscapeIdentifier .DatabaseName}}"."{{EscapeIdentifier .Name}}";`)
