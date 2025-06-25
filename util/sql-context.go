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

func SetSqlContext(ctx context.Context, conn *sql.Conn, dbName, schemaName, storeName, computePoolName *string) error {
	conn.Raw(func(driverConn interface{}) error {
		rsctx := driverConn.(*gods.Conn).GetContext()
		if dbName != nil {
			rsctx.DatabaseName = dbName
		}
		if rsctx.DatabaseName != nil && schemaName != nil {
			rsctx.SchemaName = schemaName
		}
		if storeName != nil {
			rsctx.StoreName = storeName
		}
		if computePoolName != nil {
			rsctx.ComputePoolName = computePoolName
		}
		driverConn.(*gods.Conn).SetContext(rsctx)
		return nil
	})
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

	return ctx, conn, nil
}
