// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import "database/sql"

type DeltaStreamProviderCfg struct {
	Conn *sql.Conn
	Role string
}
