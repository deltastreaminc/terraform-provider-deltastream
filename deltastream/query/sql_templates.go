package query

import "github.com/deltastreaminc/terraform-provider-deltastream/util"

var describeQueryTmpl = util.ParseTemplate(`DESCRIBE {{.SQL}}`)
var lookupQuery = util.ParseTemplate(`select name, "version", intended_state, current_state, "owner", created_at, updated_at from deltastream.sys."queries" where id = '{{.ID}}';`)
var lookupQueryState = util.ParseTemplate(`select current_state from deltastream.sys."queries" where id = '{{.ID}}';`)
var terminateQuery = util.ParseTemplate(`TERMINATE QUERY {{.ID}};`)
