"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

from django.test import TestCase

from django.core.cache import get_cache

import mock

import pycassa
from pycassa.system_manager import *

class CacheTests(TestCase):
        
    def test_cache_only(self):
        cache = get_cache('cacheandra.cacheandra.CacheBackend', **{
            'LOCATION': '127.0.0.1:11211',
        })
        self.cache_tests(cache)

    def test_cache_and_cassandra(self):
        cache = get_cache('cacheandra.cacheandra.CacheBackend', **{
            'LOCATION': '127.0.0.1:11211',
            'CASSANDRA': '127.0.0.1',
        })
        self.cache_tests(cache)

    def test_cassandra_only(self):
        cache = get_cache('cacheandra.cacheandra.CacheBackend', **{
            'CASSANDRA': '127.0.0.1',
        })
        self.cache_tests(cache)
        
    def test_nothing(self):
        cache = get_cache('cacheandra.cacheandra.CacheBackend', **{
        })
        value = cache.get('testkey')
        self.assertEquals(value,None)
        result = cache.add('testkey','testvalue')
        self.assertEquals(result,None)
        value = cache.get('testkey')
        self.assertEquals(value,None)
        cache.set('testkey','secondtestvalue')
        value = cache.get('testkey')
        self.assertEquals(value,None)
        cache.delete('testkey')
        value = cache.get('testkey')
        self.assertEquals(value,None)
        cache.set('testcounter',1)
        value = cache.get('testcounter')
        self.assertEquals(value,None)
        cache.delete('testcounter')
        value = cache.get('testcounter')
        self.assertEquals(value,None)

    def cache_tests(self,cache):
        # perform a consistent set of tests on all cache configurations
        cache.clear()
        value = cache.get('testkey')
        self.assertEquals(value,None)
        result = cache.add('testkey','testvalue')
        self.assertEquals(result,True)
        value = cache.get('testkey')
        self.assertEquals(value,'testvalue')
        cache.set('testkey','secondtestvalue')
        value = cache.get('testkey')
        self.assertEquals(value,'secondtestvalue')
        cache.delete('testkey')
        value = cache.get('testkey')
        self.assertEquals(value,None)
        cache.set('testcounter',1)
        value = cache.get('testcounter')
        self.assertEquals(value,1)
        cache.incr('testcounter',delta=1)
        value = cache.get('testcounter')
        self.assertEquals(value,2)
        cache.incr('testcounter',delta=10)
        value = cache.get('testcounter')
        self.assertEquals(value,12)
        cache.decr('testcounter',delta=1)
        value = cache.get('testcounter',11)
        cache.decr('testcounter',delta=7)
        value = cache.get('testcounter')
        self.assertEquals(value,4)
        cache.delete('testcounter')
        value = cache.get('testcounter')
        self.assertEquals(value,None)
    
    def test_basic_cache(self):
        cache = get_cache('default')
        cache.clear()
        value = cache.get('testkey')
        self.assertEquals(value,None)
        result = cache.add('testkey','testvalue')
        self.assertEquals(result,True)
        value = cache.get('testkey')
        self.assertEquals(value,'testvalue')
        cache.set('testkey','secondtestvalue')
        value = cache.get('testkey')
        self.assertEquals(value,'secondtestvalue')
        cache.delete('testkey')
        value = cache.get('testkey')
        self.assertEquals(value,None)
        cache.set('testcounter',1)
        value = cache.get('testcounter')
        self.assertEquals(value,1)
        cache.incr('testcounter',delta=1)
        value = cache.get('testcounter')
        self.assertEquals(value,2)
        cache.incr('testcounter',delta=10)
        value = cache.get('testcounter')
        self.assertEquals(value,12)
        cache.decr('testcounter',delta=1)
        value = cache.get('testcounter',11)
        cache.decr('testcounter',delta=7)
        value = cache.get('testcounter')
        self.assertEquals(value,4)
        cache.delete('testcounter')
        value = cache.get('testcounter')
        self.assertEquals(value,None)
        
        