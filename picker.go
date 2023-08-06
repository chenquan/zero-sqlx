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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/sqlx"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/timex"
)

const (
	decayTime       = int64(time.Second * 10) // default value from finagle
	forcePick       = int64(time.Second)
	initSuccess     = 1000
	throttleSuccess = initSuccess / 2
	penalty         = int64(math.MaxInt32)
	pickTimes       = 3
	logInterval     = time.Minute
)

var ErrNoFollowerAvailable = errors.New("no follower available")

type (
	picker interface {
		pick() (*pickResult, error)
	}
	pickResult struct {
		conn       sqlx.SqlConn
		done       func(err error)
		followerDB int
	}
	p2cPicker struct {
		conns  []*subConn
		r      *rand.Rand
		stamp  *syncx.AtomicDuration
		lock   sync.Mutex
		accept func(err error) bool
	}
)

func newP2cPicker(followers []sqlx.SqlConn, accept func(err error) bool) *p2cPicker {
	conns := make([]*subConn, 0, len(followers))
	for i, follower := range followers {
		conns = append(conns, &subConn{
			success: initSuccess,
			db:      i,
			conn:    follower,
		})
	}

	return &p2cPicker{
		conns:  conns,
		r:      rand.New(rand.NewSource(time.Now().UnixNano())),
		stamp:  syncx.NewAtomicDuration(),
		accept: accept,
	}
}

func (p *p2cPicker) pick() (*pickResult, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	var chosen *subConn
	switch len(p.conns) {
	case 0:
		return nil, ErrNoFollowerAvailable
	case 1:
		chosen = p.choose(p.conns[0], nil)
	case 2:
		chosen = p.choose(p.conns[0], p.conns[1])
	default:
		var node1, node2 *subConn
		for i := 0; i < pickTimes; i++ {
			a := p.r.Intn(len(p.conns))
			b := p.r.Intn(len(p.conns) - 1)
			if b >= a {
				b++
			}
			node1 = p.conns[a]
			node2 = p.conns[b]
			if node1.healthy() && node2.healthy() {
				break
			}
		}

		chosen = p.choose(node1, node2)
	}

	atomic.AddInt64(&chosen.inflight, 1)
	atomic.AddInt64(&chosen.requests, 1)

	return &pickResult{
		conn:       chosen.conn,
		done:       p.buildDoneFunc(chosen),
		followerDB: chosen.db,
	}, nil
}

func (p *p2cPicker) buildDoneFunc(c *subConn) func(err error) {
	start := int64(timex.Now())
	return func(err error) {
		// 正在处理的请求数减 1
		atomic.AddInt64(&c.inflight, -1)
		now := timex.Now()
		// 保存本次请求结束时的时间点，并取出上次请求时的时间点
		last := atomic.SwapInt64(&c.last, int64(now))
		// td计算两次请求的时间间隔
		td := int64(now) - last
		if td < 0 {
			td = 0
		}

		// 用牛顿冷却定律中的衰减函数模型计算EWMA算法中的β值
		beta := math.Exp(float64(-td) / float64(decayTime))
		// 保存本次请求的耗时
		lag := int64(now) - start
		if lag < 0 {
			lag = 0
		}
		olag := atomic.LoadUint64(&c.lag)
		if olag == 0 {
			beta = 0
		}
		// 计算 EWMA 值
		atomic.StoreUint64(&c.lag, uint64(float64(olag)*beta+float64(lag)*(1-beta)))
		success := initSuccess

		if err != nil && !p.acceptable(err) {
			success = 0
		}

		osucc := atomic.LoadUint64(&c.success)
		// 指数移动加权平均法计算健康状态
		atomic.StoreUint64(&c.success, uint64(float64(osucc)*beta+float64(success)*(1-beta)))

		stamp := p.stamp.Load()
		if now-stamp >= logInterval {
			if p.stamp.CompareAndSwap(stamp, now) {
				p.logStats()
			}
		}
	}
}

func (p *p2cPicker) choose(c1, c2 *subConn) *subConn {
	start := int64(timex.Now())
	if c2 == nil {
		atomic.StoreInt64(&c1.pick, start)
		return c1
	}

	if c1.load() > c2.load() {
		c1, c2 = c2, c1
	}

	pick := atomic.LoadInt64(&c2.pick)
	if start-pick > forcePick && atomic.CompareAndSwapInt64(&c2.pick, pick, start) {
		return c2
	}

	atomic.StoreInt64(&c1.pick, start)
	return c1
}

func (p *p2cPicker) logStats() {
	p.lock.Lock()
	defer p.lock.Unlock()
	stats := make([]string, 0, len(p.conns))
	for _, conn := range p.conns {
		stats = append(stats, fmt.Sprintf("db: %d, load: %d, reqs: %d",
			conn.db, conn.load(), atomic.SwapInt64(&conn.requests, 0)))
	}

	logx.Statf("follower db - p2c - %s", strings.Join(stats, "; "))
}

type subConn struct {
	lag      uint64 // 用来保存 ewma 值(平均请求耗时)
	inflight int64  // 用在保存当前节点正在处理的请求总数
	success  uint64 // 用来标识一段时间内此连接的健康状态
	requests int64  // 用来保存请求总数
	last     int64  // 用来保存上一次请求耗时, 用于计算 ewma 值
	pick     int64  // 保存上一次被选中的时间点
	db       int
	conn     sqlx.SqlConn
}

func (c *subConn) healthy() bool {
	return atomic.LoadUint64(&c.success) > throttleSuccess
}

func (c *subConn) load() int64 {
	// ewma 相当于平均请求耗时，inflight 是当前节点正在处理请求的数量，相乘大致计算出了当前节点的网络负载

	// plus one to avoid multiply zero
	lag := int64(math.Sqrt(float64(atomic.LoadUint64(&c.lag) + 1)))
	load := lag * (atomic.LoadInt64(&c.inflight) + 1)
	if load == 0 {
		return penalty
	}

	return load
}

func (p *p2cPicker) acceptable(err error) bool {
	ok := err == nil || err == sql.ErrNoRows || err == sql.ErrTxDone || err == context.Canceled
	if p.accept == nil {
		return ok
	}

	return ok || p.accept(err)
}
