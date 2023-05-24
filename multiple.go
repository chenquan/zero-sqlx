package sqlx

import (
	"context"
	"database/sql"
	"strings"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"github.com/zeromicro/go-zero/core/trace"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const spanName = "multipleSql"

var DBTypeAttributeKey = attribute.Key("multipleSql.leader")
var followerDBSqlAttributeKey = attribute.Key("multipleSql.follower")
var (
	leaderTypeAttributeKey   = DBTypeAttributeKey.String("leader")
	followerTypeAttributeKey = DBTypeAttributeKey.String("follower")
)

var _ sqlx.SqlConn = (*multipleSqlConn)(nil)

type (
	DBConf struct {
		Leader    string
		Followers []string `json:",optional"`
		// BackLeader back to Leader when all Followers are not available.
		BackLeader        bool          `json:",optional"`
		FollowerHeartbeat time.Duration `json:",default=60s"`
	}

	SqlOption func(*multipleSqlConn)

	multipleSqlConn struct {
		leader         sqlx.SqlConn
		enableFollower bool
		p2cPicker      atomic.Value // picker
		followers      []sqlx.SqlConn
		conf           DBConf
		accept         func(error) bool
	}
)

// NewMultipleSqlConn returns a SqlConn that supports leader-follower read/write separation.
func NewMultipleSqlConn(driverName string, conf DBConf, opts ...SqlOption) sqlx.SqlConn {
	leader := sqlx.NewSqlConn(driverName, conf.Leader)
	followers := make([]sqlx.SqlConn, 0, len(conf.Followers))
	for _, datasource := range conf.Followers {
		followers = append(followers, sqlx.NewSqlConn(driverName, datasource))
	}

	conn := &multipleSqlConn{
		leader:         leader,
		enableFollower: len(followers) != 0,
		followers:      followers,
		conf:           conf,
	}

	for _, opt := range opts {
		opt(conn)
	}

	conn.p2cPicker.Store(newP2cPicker(followers, conn.accept))

	ctx, cancelFunc := context.WithCancel(context.Background())
	proc.AddShutdownListener(func() {
		cancelFunc()
	})

	if conn.enableFollower {
		go conn.startFollowerHeartbeat(ctx)
	}

	return conn
}

func (m *multipleSqlConn) Exec(query string, args ...any) (sql.Result, error) {
	return m.ExecCtx(context.Background(), query, args...)
}

func (m *multipleSqlConn) ExecCtx(ctx context.Context, query string, args ...any) (sql.Result, error) {
	ctx, span := startSpanWithLeader(ctx)
	defer span.End()
	return m.leader.ExecCtx(ctx, query, args...)
}

func (m *multipleSqlConn) Prepare(query string) (sqlx.StmtSession, error) {
	return m.PrepareCtx(context.Background(), query)
}

func (m *multipleSqlConn) PrepareCtx(ctx context.Context, query string) (sqlx.StmtSession, error) {
	ctx, span := startSpanWithLeader(ctx)
	defer span.End()
	return m.leader.PrepareCtx(ctx, query)
}

func (m *multipleSqlConn) QueryRow(v any, query string, args ...any) error {
	return m.QueryRowCtx(context.Background(), v, query, args...)
}

func (m *multipleSqlConn) QueryRowCtx(ctx context.Context, v any, query string, args ...any) error {
	db := m.getQueryDB(ctx, query)

	return db.query(func(conn sqlx.SqlConn) error {
		return conn.QueryRowCtx(ctx, v, query, args...)
	})
}

func (m *multipleSqlConn) QueryRowPartial(v any, query string, args ...any) error {
	return m.QueryRowPartialCtx(context.Background(), v, query, args...)
}

func (m *multipleSqlConn) QueryRowPartialCtx(ctx context.Context, v any, query string, args ...any) error {
	db := m.getQueryDB(ctx, query)
	return db.query(func(conn sqlx.SqlConn) error {
		return conn.QueryRowPartialCtx(ctx, v, query, args...)
	})
}

func (m *multipleSqlConn) QueryRows(v any, query string, args ...any) error {
	return m.QueryRowsCtx(context.Background(), v, query, args...)
}

func (m *multipleSqlConn) QueryRowsCtx(ctx context.Context, v any, query string, args ...any) error {
	db := m.getQueryDB(ctx, query)
	return db.query(func(conn sqlx.SqlConn) error {
		return conn.QueryRowsCtx(ctx, v, query, args...)
	})
}

func (m *multipleSqlConn) QueryRowsPartial(v any, query string, args ...any) error {
	return m.QueryRowsPartialCtx(context.Background(), v, query, args...)
}

func (m *multipleSqlConn) QueryRowsPartialCtx(ctx context.Context, v any, query string, args ...any) error {
	db := m.getQueryDB(ctx, query)
	return db.query(func(conn sqlx.SqlConn) error {
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
	ctx, span := startSpanWithLeader(ctx)
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

func (m *multipleSqlConn) getQueryDB(ctx context.Context, query string) (db queryDB) {
	defer func() {
		var span oteltrace.Span
		if db.leader {
			ctx, span = startSpanWithLeader(ctx)
		} else {
			ctx, span = startSpanWithFollower(ctx, db.followerDB)
		}
		defer span.End()
	}()

	if !m.enableFollower {
		return queryDB{conn: m.leader, leader: true}
	}

	if !m.containSelect(query) {
		return queryDB{conn: m.leader, leader: true}
	}

	result, err := m.p2cPicker.Load().(picker).pick()
	if err == nil {
		return queryDB{
			conn:       result.conn,
			done:       result.done,
			followerDB: result.followerDB,
		}
	}

	if !m.conf.BackLeader {
		return queryDB{error: err}
	}

	return queryDB{conn: m.leader, leader: true}
}

func (m *multipleSqlConn) heartbeat() {
	conns := make([]sqlx.SqlConn, 0, len(m.followers))
	for i, follower := range m.followers {
		err := pingDB(follower)
		if err != nil {
			logx.Errorw("follower db heartbeat failure, it will be automatically removed", logx.Field("err", err), logx.Field("db", i))

			continue
		}

		conns = append(conns, follower)
	}

	m.p2cPicker.Store(newP2cPicker(conns, m.accept))
}

func (m *multipleSqlConn) startFollowerHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(m.conf.FollowerHeartbeat)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.heartbeat()
		}
	}
}

func startSpan(ctx context.Context) (context.Context, oteltrace.Span) {
	tracer := trace.TracerFromContext(ctx)
	ctx, span := tracer.Start(ctx, spanName, oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	return ctx, span
}

func startSpanWithLeader(ctx context.Context) (context.Context, oteltrace.Span) {
	ctx, span := startSpan(ctx)
	span.SetAttributes(leaderTypeAttributeKey)
	return ctx, span
}

func startSpanWithFollower(ctx context.Context, db int) (context.Context, oteltrace.Span) {
	ctx, span := startSpan(ctx)
	span.SetAttributes(followerTypeAttributeKey)
	span.SetAttributes(followerDBSqlAttributeKey.Int(db))
	return ctx, span
}

// -------------

type queryDB struct {
	conn       sqlx.SqlConn
	error      error
	done       func(err error)
	leader     bool
	followerDB int
}

func (q *queryDB) query(query func(conn sqlx.SqlConn) error) (err error) {
	if q.error != nil {
		return q.error
	}
	defer func() {
		if q.done != nil {
			q.done(err)
		}
	}()

	return query(q.conn)
}

func pingDB(conn sqlx.SqlConn) error {
	return pingCtxDB(context.Background(), conn)
}

func pingCtxDB(ctx context.Context, conn sqlx.SqlConn) error {
	db, err := conn.RawDB()
	if err != nil {
		return err
	}

	return db.PingContext(ctx)
}

// -------------

func WithAccept(accept func(err error) bool) SqlOption {
	return func(conn *multipleSqlConn) {
		conn.accept = accept
	}
}
