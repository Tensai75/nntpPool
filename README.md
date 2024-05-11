nntpPool.go
=======

An NNTP connection pool package for go (golang) using  [Tensai75/nntp](github.com/Tensai75/nntp) for the NNTP connections.

Example
-------

```go
	// create a pool
	pool, err := nntpPool.New(&nntpPool.Config{
		Host:         "news.newshosting.com",
		Port:         119,
		SSL:          false,
		SkipSSLCheck: true,
		User:         "username",
		Pass:         "password",
		ConnRetries:  3,
		ConnWaitTime: 10,
		MinConns:     0,
		MaxConns:     20,
		IdleTimeout:  30,
	})
	if err != nil {
		log.Fatal("unable to create the connection pool")
	}

	// get a connection from the pool
	conn, err := pool.Get(context.TODO())
	if err != nil {
		log.Fatal("unable to get a connection from the pool")
	}

	// return the connection to the pool
	pool.Put(conn)
```
