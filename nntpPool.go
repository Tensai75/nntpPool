package nntpPool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tensai75/nntp"
)

var (
	WarnChan = make(chan error, 10)
	LogChan  = make(chan string, 10)
)

var (
	errMaxConnsShouldBePositive = errors.New("max conns should be greater than 0")
	errMinIsGreaterThanMax      = errors.New("min conns should be lower than or equal to max conns")
	errPoolAlreadyClosed        = errors.New("connection pool already closed")
	errNoConnectionsAvailable   = errors.New("no connections available")
	errConnIsNil                = errors.New("the connection is nil")
)

type ConnectionPool interface {
	// Returns number of used and total opened connections.
	Conns() (uint32, uint32)
	// Retrieves connection from pool if it exists or opens new connection.
	Get(ctx context.Context) (*NNTPConn, error)
	// Returns connection to pool.
	Put(conn *NNTPConn) error
	// Closes all connections and pool.
	Close() error
}

type Config struct {
	// Usenet server host name or IP address
	Host string
	// Usenet server port number
	Port uint32
	// Use SSL if set to true
	SSL bool
	// Skip SSL certificate check
	SkipSSLCheck bool
	// Username to connect to the usenet server
	User string
	// Password to connect to the usenet server
	Pass string
	// Number of retries per connection upon connection error
	ConnRetries uint32
	// Time to wait in seconds before trying to re-connect
	ConnWaitTime time.Duration
	// Min number of connections that will be opened during New() function.
	MinConns uint32
	// Max number of opened connections.
	MaxConns uint32
	// Duartion after idle connections will be closed
	IdleTimeout time.Duration
	// Number of cumulative connection errors after which the pool fails if no connection could be established at all
	MaxConnErrors uint32
	// Number of max "too many connections" errors after which MaxConns is automatically reduced (0 = disabled).
	// AutoReduceMaxErrors uint32
}

type NNTPConn struct {
	*nntp.Conn
	timestamp time.Time
}

type connectionPool struct {
	connsMutex  sync.RWMutex
	connsChan   chan NNTPConn
	addConnChan chan connRequest

	host          string
	port          uint32
	ssl           bool
	skipSslCheck  bool
	user          string
	pass          string
	connRetries   uint32
	connWaitTime  time.Duration
	maxRetryErros uint32
	minConns      uint32
	maxConns      uint32
	idleTimeout   time.Duration

	conns        atomic.Int32
	connAttempts atomic.Int32
	retryErrors  atomic.Int32
	lastConnTime time.Time
	closed       bool
	fatalError   error
	serverLimit  uint32
	isRetry      bool
}

type connRequest struct {
	lastConnTime time.Time
	retryNo      uint32
	lastErr      error
}

// Opens new connection pool.
func New(cfg *Config) (ConnectionPool, error) {
	switch {
	case cfg.MaxConns == 0:
		return nil, errMaxConnsShouldBePositive
	case cfg.MinConns > cfg.MaxConns:
		return nil, errMinIsGreaterThanMax
	}

	pool := &connectionPool{
		connsMutex:  sync.RWMutex{},
		connsChan:   make(chan NNTPConn, cfg.MaxConns),
		addConnChan: make(chan connRequest, 1),

		host:         cfg.Host,
		port:         cfg.Port,
		ssl:          cfg.SSL,
		skipSslCheck: cfg.SkipSSLCheck,
		user:         cfg.User,
		pass:         cfg.Pass,
		connRetries:  cfg.ConnRetries,
		connWaitTime: cfg.ConnWaitTime,
		maxConns:     cfg.MaxConns,
		minConns:     cfg.MinConns,
		idleTimeout:  cfg.IdleTimeout,

		closed:       false,
		lastConnTime: time.Now(),
		fatalError:   nil,
		serverLimit:  0,
		isRetry:      false,
	}

	go pool.addConnHandler()

	for i := uint32(0); i < pool.minConns; i++ {
		pool.addConn()
	}

	for {
		if uint32(pool.conns.Load()) >= pool.minConns {
			select {
			case LogChan <- fmt.Sprintf("pool created successfully (%v of %v connections available)", pool.conns.Load(), pool.maxConns):
			default:
			}
			return pool, nil
		}
		if uint32(pool.retryErrors.Load()) > pool.maxRetryErros {
			if pool.conns.Load() > 0 {
				select {
				case LogChan <- fmt.Sprintf("pool created with errors (%v of %v connections available)", pool.conns.Load(), pool.maxConns):
				default:
				}
				return pool, nil
			}
		}
	}
}

// Retrieves connection from the pool if it exists or opens new connection.
func (cp *connectionPool) Get(ctx context.Context) (*NNTPConn, error) {
	for {
		if cp.fatalError != nil {
			cp.Close()
			return nil, errNoConnectionsAvailable
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case conn, ok := <-cp.connsChan:
			if !ok {
				return nil, errPoolAlreadyClosed
			}
			if !cp.checkConnIsHealthy(conn) {
				continue
			}
			return &conn, nil

		default:
			go cp.addConn()
			conn, ok := <-cp.connsChan
			if !ok {
				return nil, errPoolAlreadyClosed
			}
			if !cp.checkConnIsHealthy(conn) {
				continue
			}
			return &conn, nil
		}
	}
}

// Returns connection to the pool.
func (cp *connectionPool) Put(conn *NNTPConn) error {
	select {
	case cp.connsChan <- NNTPConn{Conn: conn.Conn, timestamp: time.Now()}:
		return nil
	default:
		select {
		case LogChan <- "closing connection because channel is full":
		default:
		}
		return cp.closeConn(conn)
	}
}

// Closes all connections and pool.
func (cp *connectionPool) Close() error {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if cp.closed {
		return nil
	}
	cp.closed = true

	if cp.connsChan == nil {
		return nil
	}
	close(cp.connsChan)

	var err error
	for conn := range cp.connsChan {
		err = conn.close()
	}

	return err
}

// Returns number of opened connections.
func (cp *connectionPool) Conns() (uint32, uint32) {
	conns := uint32(cp.conns.Load())
	return conns - uint32(len(cp.connsChan)), conns
}

// factory function for the new nntp connections
func (cp *connectionPool) factory() (*nntp.Conn, error) {
	var conn *nntp.Conn
	var err error
	if cp.ssl {
		sslConfig := tls.Config{
			InsecureSkipVerify: cp.skipSslCheck,
		}
		conn, err = nntp.DialTLS("tcp", fmt.Sprintf("%v:%v", cp.host, cp.port), &sslConfig)
	} else {
		conn, err = nntp.Dial("tcp", fmt.Sprintf("%v:%v", cp.host, cp.port))
	}
	if err != nil {
		if conn != nil {
			conn.Quit()
		}
		return nil, err
	}
	if err = conn.Authenticate(cp.user, cp.pass); err != nil {
		if conn != nil {
			conn.Quit()
		}
		return nil, err
	}
	return conn, nil
}

func (cp *connectionPool) addConn() {
	cp.addConnChan <- connRequest{retryNo: 0, lastErr: nil}
}

func (cp *connectionPool) closeConn(conn *NNTPConn) error {
	cp.conns.Add(-1)
	select {
	case LogChan <- fmt.Sprintf("%v of %v connections available", cp.conns.Load(), cp.maxConns):
	default:
	}
	return conn.close()
}

func (cp *connectionPool) checkConnIsHealthy(conn NNTPConn) bool {
	// closing expired connection
	if cp.idleTimeout > 0 &&
		conn.timestamp.Add(cp.idleTimeout).Before(time.Now()) {
		select {
		case LogChan <- "closing expired connection":
		default:
		}
		cp.closeConn(&conn)
		return false
	}
	// closing unhealthy connection
	if err := conn.ping(); err != nil {
		select {
		case LogChan <- "closing unhealthy connection":
		default:
		}
		cp.closeConn(&conn)
		return false
	}
	return true
}

func (cp *connectionPool) addConnHandler() {
	for connRequest := range cp.addConnChan {

		// check connection attempts
		connAttempts := cp.connAttempts.Add(1)
		if connAttempts > int32(cp.maxConns) || (cp.serverLimit != 0 && connAttempts > int32(cp.serverLimit)) {
			// too many connection attempts
			cp.connAttempts.Add(-1)
			continue
		}

		// check if it is a retry
		if connRequest.retryNo > 0 {
			select {
			case LogChan <- fmt.Sprintf("waiting %v seconds for next connection retry", cp.connWaitTime):
			default:
			}
			time.Sleep(time.Until(connRequest.lastConnTime.Add(cp.connWaitTime)))
		}

		// try to open the connection
		conn, err := cp.factory()

		// connection error
		if err != nil {
			cp.connAttempts.Add(-1)
			select {
			case WarnChan <- err:
			default:
			}
			if connRequest.retryNo > cp.connRetries {
				if err.Error() == "482 too many connections for your user" && cp.conns.Load() > 0 {
					select {
					case LogChan <- fmt.Sprintf("reducing max connections setting to %v due to repeated 'too many connections' error", cp.conns.Load()):
					default:
					}
					cp.serverLimit = uint32(cp.conns.Load())
				} else {
					cp.fatalError = err
				}
			}
		} else {
			select {
			case LogChan <- fmt.Sprintf("connection took %v", time.Since(cp.lastConnTime)):
			default:
			}
			// connection successful
			cp.connsChan <- NNTPConn{
				Conn:      conn,
				timestamp: time.Now(),
			}
			cp.conns.Add(1)
			select {
			case LogChan <- fmt.Sprintf("new connection opened (%v of %v connections available)", cp.conns.Load(), cp.maxConns):
			default:
			}
		}

	}
}

func (c *NNTPConn) close() error {
	if c.Conn != nil {
		return c.Conn.Quit()
	}
	return nil
}

func (c *NNTPConn) ping() error {
	if c.Conn != nil {
		_, err := c.Conn.Date()
		return err
	} else {
		return errConnIsNil
	}
}
