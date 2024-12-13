package secret

import "github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

var lookupSecretTmpl = util.ParseTemplate(`SELECT type, "description", "region", "status", "owner", created_at, updated_at FROM deltastream.sys."secrets" WHERE name = '{{.Name}}';`)
var listSecretTmpl = util.ParseTemplate(`SELECT name, type, "description", "region", "status", "owner", created_at, updated_at FROM deltastream.sys."secrets";`)
var createSecretTmpl = util.ParseTemplate(`CREATE SECRET "{{.Name}}" WITH( 
	'type' = {{.Type}}, 
	{{ if .Description }}'description' = '{{.Description}}',{{ end }}
	{{ if .SecretString }}'secret_string' = '{{.SecretString}}',{{ end }}
	{{ range $k, $v := .CustomProperties }}'{{$k}}' = '{{$v}}',{{ end }}
	'access_region' = "{{.AccessRegion}}"
);`)
var dropSecretTmpl = util.ParseTemplate(`DROP SECRET "{{EscapeIdentifier .Name}}";`)
