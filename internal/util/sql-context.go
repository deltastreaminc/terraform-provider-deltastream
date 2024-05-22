// Copyright (c) DeltaStream, Inc.
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"database/sql"
	"fmt"

	gods "github.com/deltastreaminc/go-deltastream"
	"github.com/google/uuid"
	"github.com/hashicorp/terraform-plugin-log/tflog"
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

func GetConnection(ctx context.Context, db *sql.DB, sessionID *string, org, roleName string) (context.Context, *sql.Conn, error) {
	ctx = tflog.SetField(ctx, "session-id", ptr.Deref(sessionID, ""))
	conn, err := db.Conn(ctx)
	if err != nil {
		return ctx, nil, err
	}

	conn.Raw(func(driverConn interface{}) error {
		c := driverConn.(*gods.Conn)
		ctx := c.GetContext()
		ctx.OrganizationID = ptr.To(uuid.MustParse(org))
		ctx.RoleName = ptr.To(roleName)
		c.SetContext(ctx)
		return nil
	})

	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return ctx, nil, fmt.Errorf("failed to establish connection: %w", err)
	}

	// if _, err := conn.ExecContext(ctx, fmt.Sprintf(`USE ORGANIZATION "%s";`, org)); err != nil {
	// 	conn.Close()
	// 	return ctx, nil, fmt.Errorf("failed to set organization: %w", err)
	// }

	// sRoleName, _, _, _ := getSqlConnectionContext(conn)
	// if roleName != ptr.Deref(sRoleName, "") {
	// 	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`USE ROLE "%s";`, roleName)); err != nil {
	// 		conn.Close()
	// 		return ctx, nil, fmt.Errorf("failed to set role: %w", err)
	// 	}
	// }

	return ctx, conn, nil
}
