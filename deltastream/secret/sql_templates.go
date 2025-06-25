package secret

import "github.com/deltastreaminc/terraform-provider-deltastream/util"

var lookupSecretTmpl = util.ParseTemplate(`SELECT type, "description", "status", "owner", created_at, updated_at FROM deltastream.sys."secrets" WHERE name = '{{.Name}}';`)
var listSecretTmpl = util.ParseTemplate(`SELECT name, type, "description", "status", "owner", created_at, updated_at FROM deltastream.sys."secrets";`)
var createSecretTmpl = util.ParseTemplate(`CREATE SECRET "{{.Name}}" WITH( 
	'type' = {{.Type}}, 
	{{ if .Description }}'description' = '{{.Description}}',{{ end }}
	{{ range $k, $v := .CustomProperties }}'{{$k}}' = '{{$v}}',{{ end }}
	{{ if .SecretString }}'secret_string' = '{{.SecretString}}'{{ end }}
);`)
var dropSecretTmpl = util.ParseTemplate(`DROP SECRET "{{EscapeIdentifier .Name}}";`)
