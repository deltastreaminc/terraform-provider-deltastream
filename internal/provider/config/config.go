package config

import "database/sql"

type DeltaStreamProviderCfg struct {
	Conn *sql.Conn
	Role string
}
