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
	_, salve1Mock, err := sqlmock.NewWithDSN(follower1, sqlmock.MonitorPingsOption(true))
	_, leaderMock, err := sqlmock.NewWithDSN(leader, sqlmock.MonitorPingsOption(true))
	assert.NoError(t, err)
	salve1Mock.ExpectPing().WillDelayFor(time.Millisecond)
	leaderMock.ExpectPing().WillDelayFor(time.Millisecond)

	mysql := NewMultipleSqlConn(mockedDatasource, DBConf{
		Leader:     leader,
		Followers:  []string{follower1},
		BackLeader: false,
		Heartbeat:  time.Minute,
	})

	salve1Mock.ExpectQuery("select").WillReturnRows(
		sqlmock.NewRows([]string{"a"}).AddRow("foo"),
	)

	var result string
	err = mysql.QueryRow(&result, "select")
	assert.NoError(t, err)
	assert.EqualValues(t, "foo", result)

	var list []string
	err = mysql.QueryRows(&result, "select")
	assert.NoError(t, err)
	assert.EqualValues(t, []string{"foo"}, list)

	leaderMock.ExpectExec("any").WillReturnResult(driver.RowsAffected(1))
	r, err := mysql.Exec("any")
	assert.NoError(t, err)
	rowsAffected, err := r.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)

}
func TestNewMultipleSqlConn1(t *testing.T) {
	leader := "leader"
	follower1 := "follower1"
	_, follower1Mock, err := sqlmock.NewWithDSN(follower1, sqlmock.MonitorPingsOption(true))
	_, leaderMock, err := sqlmock.NewWithDSN(leader, sqlmock.MonitorPingsOption(true))
	assert.NoError(t, err)

	follower1Mock.ExpectPing().WillDelayFor(time.Millisecond)
	leaderMock.ExpectPing().WillDelayFor(time.Millisecond)

	mysql := NewMultipleSqlConn(mockedDatasource, DBConf{
		Leader:     leader,
		Followers:  []string{follower1},
		BackLeader: false,
		Heartbeat:  time.Minute,
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