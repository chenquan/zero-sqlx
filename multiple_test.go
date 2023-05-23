package sqlx

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
)

const mockedDatasource = "sqlmock"

func TestNewMultipleSqlConn(t *testing.T) {
	leader := "leader"
	follower1 := "follower1"
	_, follower1Mock, err := sqlmock.NewWithDSN(follower1, sqlmock.MonitorPingsOption(true))
	assert.NoError(t, err)
	_, leaderMock, err := sqlmock.NewWithDSN(leader, sqlmock.MonitorPingsOption(true))
	assert.NoError(t, err)

	follower1Mock.ExpectPing().WillDelayFor(time.Millisecond)
	leaderMock.ExpectPing().WillDelayFor(time.Millisecond)

	mysql := NewMultipleSqlConn(mockedDatasource, DBConf{
		Leader:            leader,
		Followers:         []string{follower1},
		BackLeader:        false,
		FollowerHeartbeat: time.Minute,
	})

	follower1Mock.ExpectExec("any")
	follower1Mock.ExpectQuery("any").WillReturnRows(sqlmock.NewRows([]string{"foo"}))

	var val string
	assert.NotNil(t, mysql.QueryRow(&val, "any"))
	assert.NotNil(t, mysql.QueryRow(&val, "any"))
	assert.NotNil(t, mysql.QueryRowPartial(&val, "any"))
	assert.NotNil(t, mysql.QueryRows(&val, "any"))
	assert.NotNil(t, mysql.QueryRowsPartial(&val, "any"))
	_, err = mysql.Prepare("any")
	assert.NotNil(t, err)
	assert.NotNil(t, mysql.Transact(func(session sqlx.Session) error {
		return nil
	}))

	leaderMock.ExpectExec("any").WillReturnResult(driver.RowsAffected(1))
	r, err := mysql.Exec("any")
	assert.NoError(t, err)
	rowsAffected, err := r.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)
}
