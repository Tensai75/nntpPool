package nntpPool

import (
	"crypto/tls"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tensai75/nntp"
)

type provider struct {
	name                  string
	host                  string
	port                  uint32
	ssl                   bool
	skipSslCheck          bool
	user                  string
	pass                  string
	maxConns              uint32
	connWaitTime          time.Duration
	idleTimeout           time.Duration
	healthCheck           bool
	maxTooManyConnsErrors uint32
	maxConnErrors         uint32
	connsMutex            sync.RWMutex
	conns                 uint32
	connAttempts          uint32
	fatalError            error
	serverLimit           uint32
	tooManyConnsErrors    uint32
	connErrors            uint32
	failed                bool
}

func (p *provider) addConn(pool *connectionPool) {
	go func() {
		defer func() {
			if !pool.created {
				pool.startupWG.Done()
			}
		}()

		p.connsMutex.Lock()
		if pool.closed {
			// pool is already closed
			p.connsMutex.Unlock()
			return
		}
		if p.connAttempts >= p.serverLimit {
			// ignoring the connection attempt if there are already too many attempts
			pool.debug(fmt.Sprintf("ignoring new connection attempt (current connection attempts: %v | current server limit: %v connections)", p.connAttempts, p.serverLimit))
			p.connsMutex.Unlock()
			return
		}

		// try to open the connection
		p.connAttempts++
		p.connsMutex.Unlock()
		conn, err := p.factory()
		p.connsMutex.Lock()

		// abort function for reducing conAttempts counter and closing the connection
		abort := func() {
			p.connAttempts--
			pool.connsMutex.Lock()
			pool.connAttempts--
			pool.connsMutex.Unlock()
			if conn != nil {
				conn.Quit()
			}
		}

		if pool.closed {
			// if pool was closed meanwhile, abort
			abort()
			p.connsMutex.Unlock()
			return
		}

		// connection error
		if err != nil {
			pool.warn(err)
			// abort and handle error
			abort()
			if p.maxTooManyConnsErrors > 0 && (err.Error()[0:3] == "482" || err.Error()[0:3] == "502") && p.conns > 0 {
				// handle too many connections error
				p.tooManyConnsErrors++
				if p.tooManyConnsErrors >= p.maxTooManyConnsErrors && p.serverLimit > p.conns {
					p.serverLimit = p.conns
					pool.debug(fmt.Sprintf("reducing max connections to %v due to repeated 'too many connections' error", p.serverLimit))
				}
			} else {
				// handle any other error
				if p.maxConnErrors > 0 {
					p.connErrors++
					if p.connErrors >= p.maxConnErrors && p.conns == 0 {
						p.fatalError = err
						p.failed = true
						pool.warn(fmt.Errorf("unable to establish a connection  - last error was: %v", p.fatalError))
					}
				}
			}
			p.connsMutex.Unlock()
			// retry to connect
			go func() {
				pool.debug(fmt.Sprintf("waiting %v seconds for next connection retry", p.connWaitTime))
				time.Sleep(p.connWaitTime)
				pool.addConn()
			}()
			return
		}

		// connection successfull
		p.tooManyConnsErrors = 0
		p.connErrors = 0

		select {
		// try to push connection to the connections channel
		case pool.connsChan <- NNTPConn{
			Conn:      conn,
			provider:  p,
			timestamp: time.Now(),
		}:
			p.conns++
			atomic.AddUint32(&pool.conns, 1)
			pool.debug(fmt.Sprintf("new connection opened (%v of %v connections available)", pool.conns, pool.poolLimit))

		// if the connection channel is full, abort
		default:
			abort()
		}
		p.connsMutex.Unlock()
	}()
}

func (p *provider) factory() (*nntp.Conn, error) {
	var conn *nntp.Conn
	var err error
	if p.ssl {
		sslConfig := tls.Config{
			InsecureSkipVerify: p.skipSslCheck,
		}
		conn, err = nntp.DialTLS("tcp", fmt.Sprintf("%v:%v", p.host, p.port), &sslConfig)
	} else {
		conn, err = nntp.Dial("tcp", fmt.Sprintf("%v:%v", p.host, p.port))
	}
	if err != nil {
		if conn != nil {
			conn.Quit()
		}
		return nil, err
	}
	if err = conn.Authenticate(p.user, p.pass); err != nil {
		if conn != nil {
			conn.Quit()
		}
		return nil, err
	}
	return conn, nil
}
