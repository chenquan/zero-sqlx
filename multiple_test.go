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
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
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
	rows := sqlmock.NewRows([]string{"name"}).AddRow("John Doe")
	follower1Mock.ExpectQuery("SELECT name FROM users ").WithoutArgs().WillReturnRows(rows)

	var val string
	assert.NoError(t, mysql.QueryRow(&val, "SELECT name FROM users "))
	fmt.Println(val)

	leaderMock.ExpectQuery("SELECT addr FROM users").WithoutArgs().WillReturnRows(sqlmock.NewRows([]string{"foo"}).AddRow("bar"))
	assert.NoError(t, mysql.QueryRowCtx(ForceLeaderContext(context.Background()), &val, "SELECT addr FROM users"))

	leaderMock.ExpectExec("INSERT INTO users").
		WithArgs("john").WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := mysql.Exec(`INSERT INTO users(name) VALUES (?)`, "john")
	assert.NoError(t, err)
	insertId, err := result.LastInsertId()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, insertId)
}

func TestForceLeaderContext(t *testing.T) {
	ctx := ForceLeaderContext(context.Background())
	assert.True(t, forceLeaderFromContext(ctx))

	assert.False(t, forceLeaderFromContext(context.Background()))
}
