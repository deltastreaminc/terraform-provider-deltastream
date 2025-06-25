// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package database

import "github.com/deltastreaminc/terraform-provider-deltastream/internal/util"

var lookupDatabaseTmpl = util.ParseTemplate(`SELECT "owner", created_at FROM deltastream.sys."databases" WHERE name = '{{.DatabaseName}}';`)
var listDatabasesTmpl = util.ParseTemplate(`SELECT name, "owner", created_at FROM deltastream.sys."databases";`)
var createDatabaseTmpl = util.ParseTemplate(`CREATE DATABASE "{{EscapeIdentifier .DatabaseName}}";`)
var dropDatabaseTmpl = util.ParseTemplate(`DROP DATABASE "{{EscapeIdentifier .DatabaseName}}";`)
