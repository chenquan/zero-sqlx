/*
 *    Copyright 2023 chenquan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package sqlx

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"github.com/zeromicro/go-zero/core/trace"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const spanName = "multipleSql"

var (
	dbTypeAttributeKey        = attribute.Key("multiple_sql.type")
	followerDBSqlAttributeKey = attribute.Key("multiple_sql.follower_db")
	leaderTypeAttributeKey    = dbTypeAttributeKey.String("leader")
	followerTypeAttributeKey  = dbTypeAttributeKey.String("follower")
	sqlDriverAttributeKey     = attribute.Key("sql.driver")
)

var _ sqlx.SqlConn = (*multipleSqlConn)(nil)

type (
	FollowerDB struct {
		Name       string
		Datasource string
		Added      bool
	}

	DBConf struct {
		Leader       string
		Followers    []string `json:",optional"`
		BackToOrigin bool     `json:",optional"`
	}

	multipleSqlConn struct {
		leader     sqlx.SqlConn
		p2cPicker  picker // picker
		conf       DBConf
		driveName  string
		sqlOptions *sqlOptions
	}
)

// NewMultipleSqlConn returns a SqlConn that supports leader-follower read/write separation.
func NewMultipleSqlConn(driverName string, conf DBConf, opts ...SqlOption) sqlx.SqlConn {
	var sqlOpt sqlOptions
	for _, opt := range opts {
		opt(&sqlOpt)
	}

	leader := sqlx.NewSqlConn(driverName, conf.Leader, sqlx.WithAcceptable(sqlOpt.accept))

	conn := &multipleSqlConn{
		leader:     leader,
		conf:       conf,
		driveName:  driverName,
		sqlOptions: &sqlOpt,
	}

	p2cPickerObj := newP2cPicker(driverName, sqlOpt.accept)
	for i, datasource := range conf.Followers {
		p2cPickerObj.add(strconv.Itoa(i), datasource)
	}
	go func() {
		if sqlOpt.watcher == nil {
			return
		}

		for {
			select {
			case follow := <-sqlOpt.watcher:
				if follow.Added {
					p2cPickerObj.add(follow.Name, follow.Datasource)
				} else {
					p2cPickerObj.del(follow.Datasource)
				}
			}
		}
	}()

	conn.p2cPicker = p2cPickerObj

	return conn
}

func (m *multipleSqlConn) Exec(query string, args ...any) (sql.Result, error) {
	return m.ExecCtx(context.Background(), query, args...)
}

func (m *multipleSqlConn) ExecCtx(ctx context.Context, query string, args ...any) (sql.Result, error) {
	ctx, span := m.startSpanWithLeader(ctx)
	defer span.End()
	return m.leader.ExecCtx(ctx, query, args...)
}

func (m *multipleSqlConn) Prepare(query string) (sqlx.StmtSession, error) {
	return m.PrepareCtx(context.Background(), query)
}

func (m *multipleSqlConn) PrepareCtx(ctx context.Context, query string) (sqlx.StmtSession, error) {
	ctx, span := m.startSpanWithLeader(ctx)
	defer span.End()
	return m.leader.PrepareCtx(ctx, query)
}

func (m *multipleSqlConn) QueryRow(v any, query string, args ...any) error {
	return m.QueryRowCtx(context.Background(), v, query, args...)
}

func (m *multipleSqlConn) QueryRowCtx(ctx context.Context, v any, query string, args ...any) error {
	return m.query(ctx, query, func(ctx context.Context, conn sqlx.SqlConn) error {
		return conn.QueryRowCtx(ctx, v, query, args...)
	})
}

func (m *multipleSqlConn) QueryRowPartial(v any, query string, args ...any) error {
	return m.QueryRowPartialCtx(context.Background(), v, query, args...)
}

func (m *multipleSqlConn) QueryRowPartialCtx(ctx context.Context, v any, query string, args ...any) error {
	return m.query(ctx, query, func(ctx context.Context, conn sqlx.SqlConn) error {
		return conn.QueryRowPartialCtx(ctx, v, query, args...)
	})
}

func (m *multipleSqlConn) QueryRows(v any, query string, args ...any) error {
	return m.QueryRowsCtx(context.Background(), v, query, args...)
}

func (m *multipleSqlConn) QueryRowsCtx(ctx context.Context, v any, query string, args ...any) error {
	return m.query(ctx, query, func(ctx context.Context, conn sqlx.SqlConn) error {
		return conn.QueryRowsCtx(ctx, v, query, args...)
	})
}

func (m *multipleSqlConn) QueryRowsPartial(v any, query string, args ...any) error {
	return m.QueryRowsPartialCtx(context.Background(), v, query, args...)
}

func (m *multipleSqlConn) QueryRowsPartialCtx(ctx context.Context, v any, query string, args ...any) error {
	return m.query(ctx, query, func(ctx context.Context, conn sqlx.SqlConn) error {
		return conn.QueryRowsPartialCtx(ctx, v, query, args...)
	})
}

func (m *multipleSqlConn) RawDB() (*sql.DB, error) {
	return m.leader.RawDB()
}

func (m *multipleSqlConn) Transact(fn func(sqlx.Session) error) error {
	return m.TransactCtx(context.Background(), func(_ context.Context, session sqlx.Session) error {
		return fn(session)
	})
}

func (m *multipleSqlConn) TransactCtx(ctx context.Context, fn func(context.Context, sqlx.Session) error) error {
	ctx, span := m.startSpanWithLeader(ctx)
	defer span.End()
	return m.leader.TransactCtx(ctx, fn)
}

func (m *multipleSqlConn) containSelect(query string) bool {
	query = strings.TrimSpace(query)
	if len(query) >= 6 {
		return strings.EqualFold(query[:6], "select")
	}

	return false
}

func (m *multipleSqlConn) getQueryDB(ctx context.Context, query string) queryDB {
	if forceLeaderFromContext(ctx) {
		return queryDB{conn: m.leader}
	}

	if !m.containSelect(query) {
		return queryDB{conn: m.leader}
	}

	result, err := m.p2cPicker.pick()
	if err == nil {
		return queryDB{
			conn:       result.conn,
			done:       result.done,
			followerDB: result.followerDB,
			follower:   true,
		}
	}

	if !m.conf.BackToOrigin {
		return queryDB{error: err}
	}

	return queryDB{conn: m.leader}
}

func (m *multipleSqlConn) startSpan(ctx context.Context) (context.Context, oteltrace.Span) {
	tracer := trace.TracerFromContext(ctx)
	ctx, span := tracer.Start(ctx, spanName, oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	span.SetAttributes(sqlDriverAttributeKey.String(m.driveName))
	return ctx, span
}

func (m *multipleSqlConn) startSpanWithLeader(ctx context.Context) (context.Context, oteltrace.Span) {
	ctx, span := m.startSpan(ctx)
	span.SetAttributes(leaderTypeAttributeKey)
	return ctx, span
}

func (m *multipleSqlConn) startSpanWithFollower(ctx context.Context, dbName string) (context.Context, oteltrace.Span) {
	ctx, span := m.startSpan(ctx)
	span.SetAttributes(followerTypeAttributeKey)
	span.SetAttributes(followerDBSqlAttributeKey.String(dbName))
	return ctx, span
}

func (m *multipleSqlConn) query(ctx context.Context, query string, do func(ctx context.Context, conn sqlx.SqlConn) error) error {
	db := m.getQueryDB(ctx, query)
	var span oteltrace.Span
	if db.follower {
		ctx, span = m.startSpanWithFollower(ctx, db.followerDB)
	} else {
		ctx, span = m.startSpanWithLeader(ctx)
	}
	defer span.End()

	return db.query(ctx, do)
}

// -------------

type queryDB struct {
	conn       sqlx.SqlConn
	error      error
	done       func(err error)
	follower   bool
	followerDB string
}

func (q *queryDB) query(ctx context.Context, query func(ctx context.Context, conn sqlx.SqlConn) error) (err error) {
	if q.error != nil {
		return q.error
	}

	defer func() {
		if q.done != nil {
			q.done(err)
		}
	}()

	return query(ctx, q.conn)
}

type forceLeaderKey struct{}

func ForceLeaderContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, forceLeaderKey{}, struct{}{})
}

func forceLeaderFromContext(ctx context.Context) bool {
	value := ctx.Value(forceLeaderKey{})
	_, ok := value.(struct{})
	return ok
}
