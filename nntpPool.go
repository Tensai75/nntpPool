package nntpPool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Tensai75/nntp"
)

var (
	// message channels
	ErrorChan = make(chan error, 10)  // error messages (if pool has failed)
	WarnChan  = make(chan error, 10)  // warning messages (errors which did not cause the pool to fail)
	LogChan   = make(chan string, 10) // informative messages
	DebugChan = make(chan string, 10) // additional debug messages
)

var (
	errWrongConfigType  = errors.New("configuration must be type *Config or *Multiconfig")
	errNoProviderConfig = errors.New("no provider configuration provided")
	errPoolWasClosed    = errors.New("connection pool was closed")
	errConnIsNil        = errors.New("connection is nil")
)

type ConnectionPool interface {
	// Returns number of currently used and total opened connections.
	Conns() (uint32, uint32)
	// Returns the maximum number of simultaneously used connections.
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

type MultiConfig struct {
	Name      string
	Providers []*Config
}

type NNTPConn struct {
	*nntp.Conn
	provider  *provider
	timestamp time.Time
	closed    bool
}

type connectionPool struct {
	name         string
	connsMutex   sync.RWMutex
	connsChan    chan NNTPConn
	providers    []*provider
	maxConns     uint32
	conns        uint32
	connAttempts uint32
	closed       bool
	fatalError   error
	poolLimit    uint32
	startupWG    sync.WaitGroup
	created      bool
	maxConnsUsed uint32
}

// Opens new connection pool.
func New(c interface{}, initialConns uint32) (ConnectionPool, error) {

	config := new(MultiConfig)
	switch t := c.(type) {
	case *MultiConfig:
		config = t
	case *Config:
		config.Providers = append(config.Providers, t)
		config.Name = t.Name
	default:
		return nil, errWrongConfigType
	}

	if len(config.Providers) == 0 {
		return nil, errNoProviderConfig
	}

	pool := new(connectionPool)
	pool.name = config.Name
	for _, cfg := range config.Providers {
		pool.providers = append(pool.providers, &provider{
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
			serverLimit:           cfg.MaxConns,
		})
		pool.maxConns = pool.maxConns + cfg.MaxConns
	}
	pool.poolLimit = pool.maxConns
	pool.connsChan = make(chan NNTPConn, pool.maxConns)

	if initialConns > pool.poolLimit {
		initialConns = pool.poolLimit
	}

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
				if cp.fatalError != nil {
					return nil, cp.fatalError
				}
				return nil, errPoolWasClosed
			}
			if !cp.checkConnIsHealthy(conn) {
				continue
			}
			return &conn, nil

		default:
			go cp.addConn()
			conn, ok := <-cp.connsChan
			if !ok {
				if cp.fatalError != nil {
					return nil, cp.fatalError
				}
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
	if !cp.closed {
		select {
		case cp.connsChan <- NNTPConn{Conn: conn.Conn, provider: conn.provider, timestamp: time.Now()}:
			cp.connsMutex.Unlock()
			return
		default:
			cp.debug("closing returned connection because pool is full")
		}
	} else {
		cp.debug("closing returned connection because pool is closed")
	}
	cp.connsMutex.Unlock()
	cp.closeConn(conn)
}

// Closes all connections and pool.
func (cp *connectionPool) Close() {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	if cp.closed {
		// pool is already closed
		return
	}
	cp.debug("closing pool")
	if cp.connsChan != nil {
		cp.debug("closing connection channel")
		close(cp.connsChan)
		cp.debug("closing open connections")
		for conn := range cp.connsChan {
			conn.Close()
		}
	}
	cp.closed = true
	cp.conns = 0
	cp.connAttempts = 0
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
	return cp.maxConnsUsed
}

func (cp *connectionPool) addConn() {
	go func() {
		cp.connsMutex.Lock()
		if cp.closed {
			// pool is already closed
			cp.connsMutex.Unlock()
			return
		}
		if cp.connAttempts >= cp.poolLimit {
			// ignoring the connection attempt if there are already too many attempts
			cp.debug(fmt.Sprintf("ignoring new connection attempt (current connection attempts: %v | current pool limit: %v connections)", cp.connAttempts, cp.poolLimit))
			cp.connsMutex.Unlock()
			return
		}

		// try to open the connection
		cp.connAttempts++
		var failed int
		var fatalError error
		for _, provider := range cp.providers {
			provider.connsMutex.Lock()
			if provider.failed {
				failed++
				fatalError = provider.fatalError
			} else {
				if provider.connAttempts < provider.serverLimit {
					provider.connsMutex.Unlock()
					cp.connsMutex.Unlock()
					provider.addConn(cp)
					return
				}
			}
			provider.connsMutex.Unlock()
		}
		cp.connAttempts--

		// if all provider have failed closed the pool
		if len(cp.providers) == failed {
			cp.fatalError = fatalError
			cp.error(fmt.Errorf("pool failed - %v", cp.fatalError))
			cp.connsMutex.Unlock()
			cp.Close()
			return
		}
		cp.connsMutex.Unlock()
	}()
}

func (cp *connectionPool) closeConn(conn *NNTPConn) {
	cp.connsMutex.Lock()
	defer cp.connsMutex.Unlock()
	cp.conns--
	cp.connAttempts--
	conn.provider.connsMutex.Lock()
	conn.provider.conns--
	conn.provider.connAttempts--
	conn.provider.connsMutex.Unlock()
	conn.Close()
	cp.debug(fmt.Sprintf("connection closed (%v of %v connections available)", cp.conns, cp.poolLimit))
}

func (cp *connectionPool) checkConnIsHealthy(conn NNTPConn) bool {
	// closing expired connection
	if conn.provider.idleTimeout > 0 &&
		conn.timestamp.Add(conn.provider.idleTimeout).Before(time.Now()) {
		cp.debug("closing expired connection")
		cp.closeConn(&conn)
		return false
	}
	// closing unhealthy connection
	if conn.provider.healthCheck {
		if err := conn.ping(); err != nil {
			cp.debug("closing unhealthy connection")
			cp.closeConn(&conn)
			return false
		}
	}
	return true
}

func (cp *connectionPool) log(text string) {
	select {
	case LogChan <- fmt.Sprintf("%s: %s", cp.name, text):
	default:
	}
}

func (cp *connectionPool) error(err error) {
	select {
	case ErrorChan <- fmt.Errorf("%s: %v", cp.name, err):
	default:
	}
}

func (cp *connectionPool) warn(err error) {
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

func (c *NNTPConn) Close() {
	if !c.closed {
		if c.Conn != nil {
			go c.Conn.Quit()
		}
		c.closed = true
	}
}

func (c *NNTPConn) ping() error {
	if c.Conn != nil {
		_, err := c.Conn.Date()
		return err
	} else {
		return errConnIsNil
	}
}
