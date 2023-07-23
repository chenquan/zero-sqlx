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
		Leader:    leader,
		Followers: []string{follower1},
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

func TestForceLeaderContext(t *testing.T) {
	ctx := ForceLeaderContext(context.Background())
	assert.True(t, forceLeaderFromContext(ctx))

	assert.False(t, forceLeaderFromContext(context.Background()))
}
