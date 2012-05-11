cacheandra
==========

Django cache backend that includes memcached and cassandra for persistent scalable caches

What is it?

A custom cache backend for Django that uses both memcache (pylibmc) and cassandra (pycassa) to read and write values

The caching strategy is for reads:

1.  Read first from memcache
2.  if the memcache read fails try a read from cassandra
3.  if cassandra succeeds then write the value to memcache then return it

For writes

1.  Write to memcache
2.  Write to cassandra

Cassandra values are stored with an RF=1 as it's a volatile cache - we don't care about redundancy

The cache backend can run without any memcache servers (read and write just to cassandra), without any cassandra servers (read and write just to memcache) and without both (fail everything)

This allows for total flexibility in scaling up and down memcache servers without ever completing invalidating the cache.


Usage
=====

To use this you need to add a new cache backend - for a local memcached server

On cassandra you need to create the keyspaces and column families.   In cassandra-cli do:

```
create keyspace cacheandra;
use keyspace cacheandra;
create column family cache;
create column family cache_counter with default_validation_class='CounterColumnType';
```

As it's a cache a replication factor of 1 is probably fine.

```
CACHES = {
    'default': {
        'BACKEND': 'cacheandra.cacheandra.CacheBackend',
        'LOCATION': '127.0.0.1:11211',
        'CASSANDRA': '127.0.0.1',
        },
	}
```
	
Of course what's more fun is to use this with a cluster of memcached servers and a cluster of cassandra servers

Here's how the django config would look then with two clusters, 3 memcached servers and 3 cassandra servers.  
Note the pylibmc options - this is written against this fork of libmemcached 0.53  https://github.com/millarm/Libmemcached which gracefully handles individual server failures.
Works fine with stock libmemcached and pylibmc

```
CACHES = {
    'default': {
        'BACKEND': 'cacheandra.cacheandra.CacheBackend',
        'LOCATION': [
				'123.123.123.123:11211',
				'123.123.123.124:11211',
				'123.123.123.125:11211',
				],
        'CASSANDRA': [
				'213.123.123.123',
				'213.123.123.124',
				'213.123.123.125',
				],
        'OPTIONS' : dict(tcp_nodelay=True, hash='md5', distribution='consistent ketama', remove_failed=3, connect_timeout=1000),
        },
	}
```