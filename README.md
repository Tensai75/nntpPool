nntpPool.go
=======

An NNTP connection pool package for go (golang) using  [Tensai75/nntp](https://github.com/Tensai75/nntp) for the NNTP connections.

Example
-------

```go
	initialConnections := uint32(10)

	// create a pool
	pool, err := nntpPool.New(&nntpPool.Config{
		Name:                  "Newshosting",
		Host:                  "news.newshosting.com",
		Port:                  119,
		SSL:                   false,
		SkipSSLCheck:          true,
		User:                  "username",
		Pass:                  "password",
		MaxConns:              50,
		ConnWaitTime:          10,
		IdleTimeout:           30,
		HealthCheck:           true,
		MaxTooManyConnsErrors: 3,
		MaxConnErrors:         3,
	}, initialConnections)
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

	// close the pool
	pool.Close()
```

Example with multiple accounts
------------------------------

```go
	var providers []*nntpPool.Config
	providers = append(providers, &nntpPool.Config{
		Name:                  "Newshosting Account 1",
		Host:                  "news.newshosting.com",
		Port:                  119,
		SSL:                   false,
		SkipSSLCheck:          true,
		User:                  "username1",
		Pass:                  "password1",
		MaxConns:              100,
		ConnWaitTime:          10,
		IdleTimeout:           30,
		HealthCheck:           true,
		MaxTooManyConnsErrors: 3,
		MaxConnErrors:         3,
	})
	providers = append(providers, &nntpPool.Config{
		Name:                  "Newshosting Account 2",
		Host:                  "news.newshosting.com",
		Port:                  119,
		SSL:                   false,
		SkipSSLCheck:          true,
		User:                  "username2",
		Pass:                  "password2",
		MaxConns:              100,
		ConnWaitTime:          10,
		IdleTimeout:           30,
		HealthCheck:           true,
		MaxTooManyConnsErrors: 3,
		MaxConnErrors:         3,
	})

	// create a pool
	pool, err := nntpPool.New(&nntpPool.MultiConfig{
		Name:      "Newshosting Pool",
		Providers: providers,
	}, initialConnections)
	if err != nil {
		log.Fatal("unable to create the connection pool")
	}

	initialConnections := uint32(200)

	// get a connection from the pool
	conn, err := pool.Get(context.TODO())
	if err != nil {
		log.Fatal("unable to get a connection from the pool")
	}

	// return the connection to the pool
	pool.Put(conn)
	
	// close the pool
	pool.Close()
```
