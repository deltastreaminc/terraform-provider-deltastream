package relation

import "github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

var lookupRelationTmpl = util.ParseTemplate(`SELECT relation_type, fqn, "owner", "state", created_at, updated_at FROM deltastream.sys."relations" WHERE database_name = '{{ .DatabaseName }}' AND schema_name = '{{ .SchemaName }}' AND name = '{{ .Name }}';`)
var lookupRelationByFQNTmpl = util.ParseTemplate(`SELECT name, relation_type, "owner", "state", created_at, updated_at FROM deltastream.sys."relations" WHERE fqn = '{{ .FQN }}';`)
var checkRelationExistsByFQNTmpl = util.ParseTemplate(`SELECT 1 FROM deltastream.sys."relations" WHERE fqn = '{{ .FQN }}';`)
var listRelationsTmpl = util.ParseTemplate(`SELECT name, fqn, relation_type, "owner", "state", created_at, updated_at FROM deltastream.sys."relations" WHERE database_name = '{{.DatabaseName}}' AND schema_name = '{{.SchemaName}}';`)
var describeRelationTmpl = util.ParseTemplate(`DESCRIBE {{.SQL}}`)
var dropRelationTmpl = util.ParseTemplate(`DROP Relation {{.FQN}};`)
