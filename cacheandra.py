"""
Cacheandra backend
"""

import time
from threading import local

from django.core.cache.backends.base import BaseCache, InvalidCacheBackendError
from django.utils import importlib

import pylibmc, _pylibmc
import logging

import cPickle as pickle

logger = logging.getLogger(__name__)

"""
This is extended from the default django backend to handle individual cache server failures in a cluster
pylibmc doesn't do a good job of returning sensible error values here, so we do the mapping here
"""

ERROR_UNKNOWN = -1
MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY = 47
MEMCACHED_ERROR_SERVER_IS_DEAD = 35

import inspect

def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


"""
Global cassandra stuff
"""

import pycassa
from pycassa import NotFoundException, InvalidRequestException, UnavailableException, TimedOutException
from pycassa.cassandra.ttypes import AuthenticationException, AuthorizationException, SchemaDisagreementException
from pycassa.pool import AllServersUnavailable, NoConnectionAvailable, MaximumRetryException

def parseMemcachedError(exception):
    if type(exception) != _pylibmc.MemcachedError:
        return ERROR_UNKNOWN
    if len(exception.args) < 1:
        return ERROR_UNKNOWN
    if exception.args[0] == 'error 47 from memcached_set: SERVER HAS FAILED AND IS DISABLED UNTIL TIMED RETRY':
        return MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY
    if exception.args[0] == 'error 35 from memcached_set: SERVER IS MARKED DEAD':
        return MEMCACHED_ERROR_SERVER_IS_DEAD
    return ERROR_UNKNOWN

def timeout_to_ttl(timeout):
    if timeout==0 or timeout<0:
        ttl=None
    elif not isinstance(timeout, (int, long)):
        ttl=1
    else:
        ttl=timeout
    return ttl

class CacheBackend(BaseCache):
    def __init__(self, server, params):
        super(CacheBackend, self).__init__(params)
        if isinstance(server, basestring):
            self._servers = server.split(';')
        else:
            self._servers = server
        if self._servers == ['']:
            self._memcached_available=False
        else:
            self._memcached_available=True
        self._local = local()
        self.timeoutretrydelay = 0.2
        
        self._options = params.get('OPTIONS', None)
        
        self._cassandra_servers = params.get('CASSANDRA',None)
        self._keyspace = params.get('KEYSPACE','cacheandra')
        self._columnfamilyname = params.get('COLUMNFAMILY','cache')
        self._cf=None
        self._countercf=None

        if self._cassandra_servers is not None:
            try:
                self._pool = pycassa.ConnectionPool(keyspace=self._keyspace,
                                                   server_list = self._cassandra_servers,
                                                   pool_size = len(self._cassandra_servers),
                                                   timeout=0.5,
                                                   max_retries=2)

            except (InvalidRequestException, UnavailableException, TimedOutException, AuthenticationException,
                    AuthorizationException, SchemaDisagreementException, AllServersUnavailable, NoConnectionAvailable,
                    MaximumRetryException) as e:
                logger.exception('Cacheandra: failed on connection pool creation %r', e)
                self._pool = None
        
            if self._pool is not None:
                try:
                    self._cf = pycassa.ColumnFamily(self._pool,self._columnfamilyname,
                                                    write_consistency_level=pycassa.ConsistencyLevel.ONE,
                                                    read_consistency_level=pycassa.ConsistencyLevel.QUORUM)
                    self._countercf = pycassa.ColumnFamily(self._pool,self._columnfamilyname+'_counter',
                                                    default_column_validators=pycassa.types.CounterColumnType(),
                                                    write_consistency_level=pycassa.ConsistencyLevel.ONE,
                                                    read_consistency_level=pycassa.ConsistencyLevel.QUORUM)
                except (InvalidRequestException, UnavailableException, TimedOutException, AuthenticationException,
                        AuthorizationException, SchemaDisagreementException, AllServersUnavailable, NoConnectionAvailable,
                        MaximumRetryException, NotFoundException) as e:
                    self._cf = None
                    self._countercf=None
                
    @property
    def _cache(self):
        # PylibMC uses cache options as the 'behaviors' attribute.
        # It also needs to use threadlocals, because some versions of
        # PylibMC don't play well with the GIL.
        client = getattr(self._local, 'client', None)
        if client:
            return client

        client = pylibmc.Client(self._servers)
        if self._options:
            client.behaviors = self._options

        self._local.client = client

        return client

    def _get_memcache_timeout(self, timeout):
        """
        Memcached deals with long (> 30 days) timeouts in a special
        way. Call this function to obtain a safe value for your timeout.
        """
        timeout = timeout or self.default_timeout
        if timeout > 2592000: # 60*60*24*30, 30 days
            # See http://code.google.com/p/memcached/wiki/FAQ
            # "You can set expire times up to 30 days in the future. After that
            # memcached interprets it as a date, and will expire the item after
            # said date. This is a simple (but obscure) mechanic."
            #
            # This means that we have to switch to absolute timestamps.
            timeout += int(time.time())
        return timeout

    def add(self, akey, value, timeout=0, version=None):
        key = self.make_key(akey, version=version)
        rv = None
        if self._memcached_available:
            try:
                rv = self._cache.add(key, value, self._get_memcache_timeout(timeout))
            except _pylibmc.MemcachedError as error:
                error_num = parseMemcachedError(error)
                if error_num == MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY:
                    # the server will either come back to life, or die, so we try again
                    logger.info("MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY on add %s" % akey)
                    time.sleep(self.timeoutretrydelay)
                    rv = self.add(akey, value, timeout, version)
                elif error_num == MEMCACHED_ERROR_SERVER_IS_DEAD:
                    # next attempt should succeed, if not return the failure
                    rv = self._cache.add(key, value, self._get_memcache_timeout(timeout))
                else:
                    rv = False
        
        if self._cf is not None:
            if not self._memcached_available:
                try:
                    retval = self._cf.get(key=key,columns=['val'])
                    rv = False
                except NotFoundException:
                    rv = True
            if rv==True:
                try:
                    if isinstance(value, ( int, long )):
                        self._countercf.add(key=key,column='count',value=value)
                    self._cf.insert(key=key,columns={'val':pickle.dumps(value)},ttl=timeout_to_ttl(timeout))
                except Exception as e:
                    print lineno(),e
                    logger.exception("Cacheandra insert failed %r",e)
        
        return rv

    def get(self, akey, default=None, version=None):
        from polls.cache.dualcache import local_cache
        key = self.make_key(akey, version=version)
        val = None
        if self._memcached_available:
            try:
                val = self._cache.get(key)
            except _pylibmc.MemcachedError as error:
                error_num = parseMemcachedError(error)
                if error_num == MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY:
                    logger.info("MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY on get %s" % akey)
                    time.sleep(self.timeoutretrydelay)
                    val = self.get(akey,default,version)
                elif error_num == MEMCACHED_ERROR_SERVER_IS_DEAD:
                    logger.error("MEMCACHED_ERROR_SERVER_IS_DEAD on get %s" % key)
                    # server is gone, cache miss
                    val = None

        if val is None:
            # OK, this could be a cache miss, or we've lost a cache server - so let's try to get it from cass
            if self._cf is not None:
                retval = {}
                try:
                    retval = self._countercf.get(key=key,columns=['count'])
                    if 'count' in retval:
                        val=retval['count']
                except NotFoundException:
                    try:
                        retval = self._cf.get(key=key,columns=['val'])
                        val=pickle.loads(retval['val'])
                    except NotFoundException:
                        val = default
                    except Exception as e:
                        print lineno(),e
                        logger.exception("Cacheandra failed get %r",e)
                        val = default
                except Exception as e:
                    print lineno(),e
                    logger.exception("Cacheandra failed get %r",e)
                    val = default
                
                try:
                    if val is not None and self._memcached_available:
                        self._cache.set(key, val, self._get_memcache_timeout(1))
                except Exception as e:
                    print lineno(),e
                    logger.exception("Cacheandra failed insert after get %r",e)
            else:
                val=default
                
        return val

    def set(self, akey, value, timeout=0, version=None):
        key = self.make_key(akey, version=version)
        if self._memcached_available:
            try:
                self._cache.set(key, value, self._get_memcache_timeout(timeout))
            except _pylibmc.MemcachedError as error:
                error_num = parseMemcachedError(error)
                if error_num == MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY:
                    logger.info("MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY on set %s %s" % (akey, value))
                    time.sleep(self.timeoutretrydelay)
                    self.set(akey, value, timeout, version)
                elif error_num == MEMCACHED_ERROR_SERVER_IS_DEAD:
                    # next attempt should succeed, else we'll raise the error
                    logger.error("MEMCACHED_ERROR_SERVER_IS_DEAD on set %s %s" % (akey, value))
                    self._cache.set(key, value, self._get_memcache_timeout(timeout))
        
        if self._cf is not None:
            try:
                self._cf.insert(key=key,columns={'val':pickle.dumps(value)},ttl=timeout_to_ttl(timeout))
                if isinstance(value, ( int, long )):
                    try:
                        val = self._countercf.get(key=key,columns=['count'])
                        if 'count' in val:
                            value-=val['count']
                    except NotFoundException:
                        pass
                    finally:
                        self._countercf.add(key=key,column='count',value=value)
            except Exception as e:
                print timeout
                print lineno(),e
                logger.exception("Cacheandra insert failed %r",e)

    def delete(self, akey, version=None):
        key = self.make_key(akey, version=version)
        if self._memcached_available:
            try:
                self._cache.delete(key)
            except _pylibmc.MemcachedError as error:
                error_num = parseMemcachedError(error)
                if error_num == MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY:
                    logger.info("MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY on delete %s" % (akey))
                    time.sleep(self.timeoutretrydelay)
                    self.delete(akey, version)
        
        if self._cf is not None:
            try:
                self._cf.remove(key=key,columns=['val'])
            except Exception as e:
                print lineno(),e
                logger.exception("Cacheandra remove failed %r",e)
            try:
                self._countercf.remove_counter(key=key,column='count')
            except Exception as e:
                print lineno(),e
                logger.exception("Cacheandra remove_counter failed %r",e)

    def get_many(self, keys, version=None):
        new_keys = map(lambda x: self.make_key(x, version=version), keys)
        ret = None
        if self._memcached_available:
            try:
                ret = self._cache.get_multi(new_keys)
            except _pylibmc.MemcachedError as error:
                error_num = parseMemcachedError(error)
                if error_num == MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY:
                    logger.info("MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY on get_many %s" % (keys))
                    time.sleep(self.timeoutretrydelay)
                    ret = self.get_many(keys,version)
                elif error_num == MEMCACHED_ERROR_SERVER_IS_DEAD:
                    logger.error("MEMCACHED_ERROR_SERVER_IS_DEAD on get_many %s" % (keys))
                    ret = {}
            if ret:
                _ = {}
                m = dict(zip(new_keys, keys))
                for k, v in ret.items():
                    _[m[k]] = v
                ret = _

        if ret == None: # this should be impossible
            ret = {}
        
        if ret=={}:
            # empty set, let's have a go at getting the values from cassandra and caching them
            try:
                value_ret = self._cf.multiget(keys=list(new_keys))
                _ = {}
                m = dict(zip(new_keys, keys))
                for k, v in value_ret.iteritems():
                    _[m[k]] = pickle.loads(v['val'])
                blank_new_keys = []
                for k in new_keys:
                    if not k in _:
                        blank_new_keys.append(k)
                if len(blank_new_keys)>0:
                    value_ret = self._countercf.multiget(keys=blank_new_keys)
                    for k, v in value_ret.iteritems():
                        _[m[k]] = v['count']
            
                ret = _
            
                safe_data = {}
                for key, value in _.items():
                    key = self.make_key(key, version=version)
                    safe_data[key] = value
                try:
                    # we've got that missing ttl problem again
                    if self._memcached_available:
                        self._cache.set_multi(safe_data, self._get_memcache_timeout(1))
                except Exception as e:
                    print lineno(),e
                    logger.exception("Cacheandra failed in cache set_multi %r",e)
            except Exception as e:
                print lineno(),e
                logger.exception("Cacheandra failed in multiget %r",e)
        
        return ret

    def close(self, **kwargs):
        self._cache.disconnect_all()

    def incr(self, akey, delta=1, version=None):
        key = self.make_key(akey, version=version)
        val = None
        if self._memcached_available:
            try:
                val = self._cache.incr(key, delta)
            except pylibmc.NotFound:
                val = None
            except _pylibmc.MemcachedError as error:
                error_num = parseMemcachedError(error)
                if error_num == MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY:
                    logger.info("MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY on incr %s %s" % (akey, delta))
                    time.sleep(self.timeoutretrydelay)
                    self.incr(akey, delta, version)
                elif error_num == MEMCACHED_ERROR_SERVER_IS_DEAD:
                    logger.error("MEMCACHED_ERROR_SERVER_IS_DEAD on incr %s %s" % (akey, delta))
                    try:
                        self._cache.incr(key, delta)
                    except pylibmc.NotFound:
                        val = None

            if val is None:
                raise ValueError("Key '%s' not found" % key)            
        
        if self._countercf is not None:
            if not self._memcached_available:
                # we can't rely on memcached having told us if the value isn't there
                try:
                    retval = self._countercf.get(key,columns=['count'])
                except NotFoundException:
                    raise ValueError("Key '%s' not found" % key)            
            try:
                self._countercf.add(key,'count',delta)
                # we are changing the value, so remove the duplicate (now old) value from the non counter CF
                self._cf.remove(key=key,columns=['val'])
                try:
                    retval = self._countercf.get(key,columns=['count'])
                    if 'count' in retval:
                        val=retval['count']
                except Exception as e:
                    print lineno(),e
                    logger.exception("Cacheandra exception on retrieve of counter incr %r",e)
            except Exception as e:
                print lineno(),e
                logger.exception("Cacheandra exception on add %r",e)
        return val

    def decr(self, akey, delta=1, version=None):
        key = self.make_key(akey, version=version)
        val = None
        if self._memcached_available:
            try:
                val = self._cache.decr(key, delta)
            except pylibmc.NotFound:
                val = None
            except _pylibmc.MemcachedError as error:
                error_num = parseMemcachedError(error)
                if error_num == MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY:
                    logger.info("MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY on decr %s %s" % (akey, delta))
                    time.sleep(self.timeoutretrydelay)
                    self.decr(akey, delta, version)
                elif error_num == MEMCACHED_ERROR_SERVER_IS_DEAD:
                    logger.error("MEMCACHED_ERROR_SERVER_IS_DEAD on decr %s %s" % (akey, delta))
                    try:
                        self._cache.decr(key, delta)
                    except pylibmc.NotFound:
                        val = None

        if self._countercf is not None:
            if not self._memcached_available:
                # we can't rely on memcached having told us if the value isn't there
                try:
                    retval = self._countercf.get(key,columns=['count'])
                except NotFoundException:
                    raise ValueError("Key '%s' not found" % key)            
            try:
                self._countercf.add(key,'count',-delta)
                # we are changing the value, so remove the duplicate (now old) value from the non counter CF
                self._cf.remove(key=key,columns=['val'])
                try:
                    retval = self._countercf.get(key,columns=['count'])
                    if 'count' in retval:
                        val=retval['count']
                except Exception as e:
                    print lineno(),e
                    logger.exception("Cacheandra exception on retrieve of counter decr %r",e)
            except Exception as e:
                print lineno(),e
                logger.exception("Cacheandra exception on add %r",e)

        if val is None:
            raise ValueError("Key '%s' not found" % key)
        return val

    def set_many(self, data, timeout=0, version=None):
        safe_data = {}
        for key, value in data.items():
            key = self.make_key(key, version=version)
            safe_data[key] = value
        if self._memcached_available:
            try:
                self._cache.set_multi(safe_data, self._get_memcache_timeout(timeout))
            except _pylibmc.MemcachedError as error:
                error_num = parseMemcachedError(error)
                if error_num == MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY:
                    logger.info("MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY on set_many %s" % (data))
                    time.sleep(self.timeoutretrydelay)
                    self.set_many(data,timeout,version)
                elif error_num == MEMCACHED_ERROR_SERVER_IS_DEAD:
                    logger.error("MEMCACHED_ERROR_SERVER_IS_DEAD on set_many %s" % (data))
                    self._cache.set_multi(safe_data, self._get_memcache_timeout(timeout))
        
        if self._cf is not None:
            try:
                b=self._cf.batch(queue_size=100)
                for key, value in data.items():
                    key = self.make_key(key, version=version)
                    b.insert(key=key,column={'val':pickle.dumps(value)},ttl=timeout_to_ttl(timeout))
                b.send()
            except Exception as e:
                print lineno(),e
                logger.exception("Cacheandra batch insert failed %r",e)

    def delete_many(self, keys, version=None):
        l = lambda x: self.make_key(x, version=version)
        if self._memcached_available:
            try:
                self._cache.delete_multi(map(l, keys))
            except _pylibmc.MemcachedError as error:
                error_num = parseMemcachedError(error)
                if error_num == MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY:
                    logger.info("MEMCACHED_ERROR_SERVER_DISABLED_UNTIL_TIMED_RETRY on delete_many %s" % (keys))
                    time.sleep(self.timeoutretrydelay)
                    self.delete_many(keys, version)
                # special case, if the server has been marked dead, we don't need to worry about deleting!

        if self._cf is not None:
            try:
                b = self._cf.batch(queue_size=100)
                for key in keys:
                    b.remove(key,['val'])
                b.send()
            except Exception as e:
                print lineno(),e
                logger.exception("Cacheandra batch remove failed %r",e)

    def clear(self):
        self._cache.flush_all()
        try:
            self._cf.truncate()
        except Exception as e:
            logger.exception("Cacheandra failed in truncate %r",e)
        try:
            self._countercf.truncate()
        except Exception as e:
            logger.exception("Cacheandra failed in truncate %r",e)
        