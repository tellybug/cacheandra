"""
This file demonstrates writing tests using the unittest module. These will pass
when you run "manage.py test".

Replace this with more appropriate tests for your application.
"""

from django.test import TestCase

from django.core.cache import get_cache

import pycassa
from pycassa.system_manager import *

class CacheTests(TestCase):
    def setUp(self):
        """
        try:
            sys = SystemManager('127.0.0.1:9160')
            sys.create_keyspace('cacheandra', SIMPLE_STRATEGY, {'replication_factor': '1'})
            # this is what I want to do - but cassandra doesn't support it
            #sys.create_column_family('cacheandra', 'cache', column_validation_classes={'val':pycassa.types.BytesType(), 'count':pycassa.types.CounterColumnType()})
            sys.create_column_family('cacheandra','cache')
            sys.create_column_family('cacheandra', 'cache_counter',default_validation_class=pycassa.types.CounterColumnType())
        except Exception as e:
            print "test",e
        """    
            
    def tearDown(self):
        sys = SystemManager('127.0.0.1:9160')
        #sys.drop_keyspace('cacheandra')
    
    def test_basic_cache(self):
        cache = get_cache('default')
        cache.clear()
        value = cache.get('testkey')
        self.assertEquals(value,None)
        cache.add('testkey','testvalue')
        value = cache.get('testkey')
        self.assertEquals(value,'testvalue')
        