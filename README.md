# zero-sqlx

zero-sqlx supports leader-follower read-write separation.

## installation

```shell
go get github.com/chenquan/zero-sqlx
```

## how to use it

```yaml
DB:
  Leader: leader
  Followers:
    - follower1
    - follower2
  BackLeader: true
```

```go
type Config struct{
  DB DBConf
}
```

```go

var c config.Config
conf.MustLoad(*configFile, &c, conf.UseEnv())


mysql := NewMultipleSqlConn("mysql", c.DB)
var name string
mysql.QueryRow(&name, "SELECT name FROM user WHERE id = 1")
```