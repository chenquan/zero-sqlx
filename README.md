# zero-sqlx

zero-sqlx supports leader-follower read-write separation.

## installation

```shell
go get github.com/chenquan/zero-sqlx

## features

- Full tracing
- Read and write separation
- Allows specified leader db execution
- Adaptive circuit breaker
- P2c algorithm

## features

- Full tracing
- Read and write separation
- Allows specified leader db execution
- Adaptive circuit breaker
- P2c algorithm

## how to use it

```yaml
DB:
  Leader: leader
  Followers:
    - follower1
    - follower2
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