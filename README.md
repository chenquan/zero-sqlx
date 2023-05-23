# zero-sqlx

zero-sqlx supports leader-follower read-write separation.

## installation

```shell
go get github.com/chenquan/zero-sqlx
```

## how to use it

```go
mysql := NewMultipleSqlConn("mysql", DBConf{
		Leader:            "leader",
		Followers:         []string{"follower1"},
		BackLeader:        false,
		FollowerHeartbeat: time.Minute,
	})
var name string
mysql.QueryRow(&name, "SELECT name FROM user WHERE id = 1")
```