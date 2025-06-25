// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package config

import "database/sql"

type DeltaStreamProviderCfg struct {
	Db           *sql.DB
	Organization string
	Role         string
	SessionID    *string
}
