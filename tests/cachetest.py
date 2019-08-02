#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on 2019年8月2日

@author: leon-sk
'''

import unittest
from rediscluster_cache.cache import RedisClusterCache

server = [{"host":"192.168.2.237", "port":7000}, {"host":"192.168.2.237", "port":7001}, {"host":"192.168.2.237", "port":7002}, {"host":"192.168.2.237", "port":7003}, {"host":"192.168.2.237", "port":7004}, {"host":"192.168.2.237", "port":7005}]
params = {"TIMEOUT":100, "OPTIONS":{"CHECK_INTERVAL":100, "SERIALIZER":"rediscluster_cache.serializers.pickle.PickleSerializer", "COMPRESSOR":"rediscluster_cache.compressors.zlib.ZlibCompressor"}}
cache = RedisClusterCache( server, params )
key = "rediscluster_cache"
key1 = "rediscluster_cache1"
key2 = "rediscluster_cache2"

    
class TestCache( unittest.TestCase ):

    def setUp( self ):
        self.cache_set()
        self.cache_get()

    def tearDown( self ):
        pass

    def cache_set( self ):
        print cache.set( key, "---success---" )
        print cache.set( key1, {"hi":"world", 1:"dfs", 2:0.999} )
        print cache.set( key2, [{"hi":"world", 1:"dfs", 2:0.999}] )

    def cache_get( self ):
        print cache.get( key )
        print cache.get( key1 )
        print cache.get( key2 )


if __name__ == '__main__':
    unittest.main()
