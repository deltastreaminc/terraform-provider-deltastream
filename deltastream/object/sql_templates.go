package object

import "github.com/deltastreaminc/terraform-provider-deltastream/util"

var lookupObjectTmpl = util.ParseTemplate(`SELECT relation_type, fqn, "owner", "state", created_at, updated_at FROM deltastream.sys."relations" WHERE database_name = '{{ .DatabaseName }}' AND schema_name = '{{ .NamespaceName }}' AND name = '{{ .Name }}';`)
var lookupObjectByFQNTmpl = util.ParseTemplate(`SELECT name, relation_type, "owner", "state", created_at, updated_at FROM deltastream.sys."relations" WHERE fqn = '{{ .FQN }}';`)
var lookupObjectByNameTmpl = util.ParseTemplate(`SELECT name, relation_type, fqn, "owner", "state", created_at, updated_at FROM deltastream.sys."relations" WHERE database_name = '{{ .DatabaseName }}' AND schema_name = '{{ .SchemaName }}' AND name = '{{ .Name }}';`)
var checkObjectExistsByFQNTmpl = util.ParseTemplate(`SELECT 1 FROM deltastream.sys."relations" WHERE fqn = '{{ .FQN }}';`)
var listObjectsTmpl = util.ParseTemplate(`SELECT name, fqn, relation_type, "owner", "state", created_at, updated_at FROM deltastream.sys."relations" WHERE database_name = '{{.DatabaseName}}' AND schema_name = '{{.NamespaceName}}';`)
var describeObjectTmpl = util.ParseTemplate(`DESCRIBE {{.SQL}}`)
var dropObjectTmpl = util.ParseTemplate(`DROP Relation {{.FQN}};`)
