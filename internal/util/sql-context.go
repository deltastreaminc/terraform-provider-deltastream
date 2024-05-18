// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"database/sql"
	"fmt"

	gods "github.com/deltastreaminc/go-deltastream"
	"k8s.io/utils/ptr"
)

func getSqlConnectionContext(conn *sql.Conn) (roleName, dbName, schemaName, storeName *string) {
	conn.Raw(func(driverConn interface{}) error {
		rsctx := driverConn.(*gods.Conn).GetContext()
		if rsctx.OrganizationID != nil {
			if rsctx.DatabaseName != nil {
				dbName = rsctx.DatabaseName
				schemaName = rsctx.SchemaName
			}
			if rsctx.StoreName != nil {
				storeName = rsctx.StoreName
			}
			if rsctx.RoleName != nil {
				roleName = rsctx.RoleName
			}
		}
		return nil
	})
	return
}

func SetSqlContext(ctx context.Context, conn *sql.Conn, roleName, dbName, schemaName, storeName *string) error {
	sRoleName, sDbName, sSchemaName, sStoreName := getSqlConnectionContext(conn)
	if roleName != nil && *roleName != ptr.Deref(sRoleName, "") {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, *roleName)); err != nil {
			return err
		}
	}
	if dbName != nil && *dbName != ptr.Deref(sDbName, "") {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`USE DATABASE "%s";`, *dbName)); err != nil {
			return err
		}
	}
	if schemaName != nil && *schemaName != ptr.Deref(sSchemaName, "") {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`USE SCHEMA "%s";`, *schemaName)); err != nil {
			return err
		}
	}
	if storeName != nil && *storeName != ptr.Deref(sStoreName, "") {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`USE STORE "%s";`, *storeName)); err != nil {
			return err
		}
	}
	return nil
}
