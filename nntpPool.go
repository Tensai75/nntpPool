package nntpPool

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tensai75/nntp"
)

var (
	WarnChan  = make(chan error, 10)
	LogChan   = make(chan string, 10)
	DebugChan = make(chan string, 10)
)

var (
	errMaxConnsShouldBePositive = errors.New("max conns should be greater than 0")
	errConsIsGreaterThanMax     = errors.New("initial amount of connections should be lower than or equal to max conns")
	errPoolWasClosed            = errors.New("connection pool was closed")
	errConnIsNil                = errors.New("connection is nil")
)

type ConnectionPool interface {
	// Returns number of used and total opened connections.
	Conns() (uint32, uint32)
	// Returns number of max opened connections.
	MaxConns() uint32
	// Retrieves connection from pool if it exists or opens new connection.
	Get(ctx context.Context) (*NNTPConn, error)
	// Returns connection to pool.
	Put(conn *NNTPConn)
	// Closes all connections and pool.
	Close()
}

type Config struct {
	// Name for the usenet server
	Name string
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
	// Max number of opened connections.
	MaxConns uint32
	// Time to wait in seconds before trying to re-connect
	ConnWaitTime time.Duration
	// Duartion after idle connections will be closed
	IdleTimeout time.Duration
	// Check health of connection befor passing it on
	HealthCheck bool
	// Number of max "too many connections" errors after which MaxConns is automatically reduced (0 = disabled)
	MaxTooManyConnsErrors uint32
	// Number of max consecutive connection errors after which the pool fails if no connection could be established at all (0 = disabled)
	MaxConnErrors uint32
}

type NNTPConn struct {
	*nntp.Conn
	timestamp time.Time
}

type connectionPool struct {
	connsMutex  sync.RWMutex
	connsChan   chan NNTPConn
	addConnChan chan connRequest

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

	conns              uint32
	connAttempts       uint32
	lastConnTime       time.Time
	closed             bool
	fatalError         error
	serverLimit        uint32
	tooManyConnsErrors uint32
	connErrors         uint32
	startupWG          sync.WaitGroup
	created            bool
	maxConnsUsed       uint32
}

type connRequest struct{}

// Opens new connection pool.
func New(cfg *Config, initialConns uint32) (ConnectionPool, error) {
	switch {
	case cfg.MaxConns <= 0:
		return nil, errMaxConnsShouldBePositive
	case initialConns > cfg.MaxConns:
		return nil, errConsIsGreaterThanMax
	}

	pool := &connectionPool{
		connsMutex:  sync.RWMutex{},
		connsChan:   make(chan NNTPConn, cfg.MaxConns),
		addConnChan: make(chan connRequest, cfg.MaxConns),

		name:                  cfg.Name,
		host:                  cfg.Host,
		port:                  cfg.Port,
		ssl:                   cfg.SSL,
		skipSslCheck:          cfg.SkipSSLCheck,
		user:                  cfg.User,
		pass:                  cfg.Pass,
		maxConns:              cfg.MaxConns,
		connWaitTime:          cfg.ConnWaitTime,
		idleTimeout:           cfg.IdleTimeout,
		healthCheck:           cfg.HealthCheck,
		maxTooManyConnsErrors: cfg.MaxTooManyConnsErrors,
		maxConnErrors:         cfg.MaxConnErrors,

		closed:       false,
		lastConnTime: time.Now(),
		fatalError:   nil,
		serverLimit:  cfg.MaxConns,
	}

	go pool.addConnHandler()

	for i := uint32(0); i < initialConns; i++ {
		pool.startupWG.Add(1)
		pool.addConn()
	}
	pool.startupWG.Wait()
	if pool.fatalError != nil {
		pool.Close()
		return nil, pool.fatalError
	}
	pool.created = true
	if initialConns > 0 && pool.conns < initialConns {
		pool.log(fmt.Sprintf("pool created with errors (%v of %v requested connections available)", pool.conns, initialConns))
	} else {
		pool.log("pool created successfully")
	}
	return pool, nil
}

// Retrieves connection from the pool if it exists or opens new connection.
func (cp *connectionPool) Get(ctx context.Context) (*NNTPConn, error) {
	defer func() {
		cp.connsMutex.Lock()
		defer cp.connsMutex.Unlock()
		usedConns := cp.conns - uint32(len(cp.connsChan))
		if usedConns > cp.maxConnsUsed {
			cp.maxConnsUsed = usedConns
		}
	}()
	for {
		if cp.fatalError != nil {
			cp.Close()
			return nil, cp.fatalError
		}
		select {
		case <-ctx.Done():
			cp.Close()
			return nil, ctx.Err()

		case conn, ok := <-cp.connsChan:
			if !ok {
				return nil, errPoolWasClosed
			}
			if !cp.checkConnIsHealthy(conn) {
				continue
			}
			return &conn, nil

		default:
			cp.connsMutex.Lock()
			if cp.connAttempts < cp.serverLimit {
				go cp.addConn()
			}
			cp.connsMutex.Unlock()
			conn, ok := <-cp.connsChan
			if !ok {
				return nil, errPoolWasClosed
			}
			if !cp.checkConnIsHealthy(conn) {
				continue
			}
			return &conn, nil
		}
	}
}

// Returns connection to the pool.
func (cp *connectionPool) Put(conn *NNTPConn) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if !cp.closed {
		select {
		case cp.connsChan <- NNTPConn{Conn: conn.Conn, timestamp: time.Now()}:
			return
		default:
			cp.debug("closing returned connection because pool is full")
		}
	} else {
		cp.debug("closing returned connection because pool is closed")
	}
	cp.closeConn(conn)
}

// Closes all connections and pool.
func (cp *connectionPool) Close() {
	cp.debug("closing pool")
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if cp.closed {
		cp.debug("pool is already closed")
		return
	}
	cp.closed = true

	if cp.connsChan != nil {
		cp.debug("closing connection channel")
		close(cp.connsChan)
		cp.debug("closing open connections")
		for conn := range cp.connsChan {
			go conn.close()
		}
	}
	if cp.addConnChan != nil {
		cp.debug("closing add connection channel")
		close(cp.addConnChan)
	}
	cp.debug("pool closed")
}

// Returns number of opened connections.
func (cp *connectionPool) Conns() (uint32, uint32) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if cp.closed {
		return 0, 0
	} else {
		conns := cp.conns
		return conns - uint32(len(cp.connsChan)), conns
	}
}

// Returns number of max used connections.
func (cp *connectionPool) MaxConns() uint32 {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	return cp.maxConnsUsed
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
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if !cp.closed {
		cp.addConnChan <- connRequest{}
	}
}

func (cp *connectionPool) closeConn(conn *NNTPConn) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	cp.conns--
	cp.connAttempts--
	cp.debug(fmt.Sprintf("%v of %v connections available", cp.conns, cp.maxConns))
	conn.close()
}

func (cp *connectionPool) checkConnIsHealthy(conn NNTPConn) bool {
	// closing expired connection
	if cp.idleTimeout > 0 &&
		conn.timestamp.Add(cp.idleTimeout).Before(time.Now()) {
		cp.debug("closing expired connection")
		cp.closeConn(&conn)
		return false
	}
	// closing unhealthy connection
	if cp.healthCheck {
		if err := conn.ping(); err != nil {
			cp.debug("closing unhealthy connection")
			cp.closeConn(&conn)
			return false
		}
	}
	return true
}

func (cp *connectionPool) addConnHandler() {
	for range cp.addConnChan {
		go func() {
			cp.connsMutex.Lock()
			if cp.connAttempts < cp.serverLimit {
				cp.connAttempts++
				cp.connsMutex.Unlock()
				connStartTime := time.Now()

				// try to open the connection
				conn, err := cp.factory()

				cp.connsMutex.Lock()
				if !cp.closed {
					if err != nil {
						// connection error
						cp.error(err)
						cp.connAttempts--
						if cp.maxTooManyConnsErrors > 0 && (err.Error()[0:3] == "482" || err.Error()[0:3] == "502") && cp.conns > 0 {
							cp.tooManyConnsErrors++
							if cp.tooManyConnsErrors >= cp.maxTooManyConnsErrors && cp.serverLimit > cp.conns {
								cp.serverLimit = cp.conns
								cp.error(fmt.Errorf("reducing max connections to %v due to repeated 'too many connections' error", cp.serverLimit))
							}
						} else {
							if cp.maxConnErrors > 0 {
								cp.connErrors++
								if cp.connErrors >= cp.maxConnErrors && cp.conns == 0 {
									cp.fatalError = fmt.Errorf("unable to establish a connection after %v retries  - last error was: %v", cp.connErrors-1, err)
									cp.error(cp.fatalError)
									cp.connsMutex.Unlock()
									cp.Close()
									return
								}
							}
						}
						go func() {
							cp.debug(fmt.Sprintf("waiting %v seconds for next connection retry", cp.connWaitTime))
							time.Sleep(cp.connWaitTime)
							cp.addConnChan <- connRequest{}
						}()
						cp.connsMutex.Unlock()
					} else {
						// connection successfull
						cp.debug(fmt.Sprintf("connection took %v", time.Since(connStartTime)))
						cp.tooManyConnsErrors = 0
						cp.connErrors = 0
						nntpConn := NNTPConn{
							Conn:      conn,
							timestamp: time.Now(),
						}
						select {
						// push connection to the connections channel
						case cp.connsChan <- nntpConn:
							cp.conns++
							cp.debug(fmt.Sprintf("new connection opened (%v of %v connections available)", cp.conns, cp.serverLimit))
							cp.connsMutex.Unlock()
							// if the connections channel is full, close the connection
						default:
							cp.connsMutex.Unlock()
							cp.closeConn(&nntpConn)
						}
					}
				} else {
					cp.connsMutex.Unlock()
					cp.closeConn(&conn)
				}

			} else {
				cp.connsMutex.Unlock()
			}
			if !cp.created {
				cp.startupWG.Done()
			}
		}()
	}
}

func (cp *connectionPool) log(text string) {
	select {
	case LogChan <- fmt.Sprintf("%s: %s", cp.name, text):
	default:
	}
}

func (cp *connectionPool) error(err error) {
	select {
	case WarnChan <- fmt.Errorf("%s: %v", cp.name, err):
	default:
	}
}

func (cp *connectionPool) debug(text string) {
	select {
	case DebugChan <- fmt.Sprintf("%s: %v", cp.name, text):
	default:
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
