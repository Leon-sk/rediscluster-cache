#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on 2019年7月25日

@author: leon-sk
'''
import random
from rediscluster_cache.pool import get_connection_factory


class NodeManager( object ):
    '''
    Managing redis cluster nodes
    '''
    Slots = 16384

    def __init__( self , server , options ):
        self._server = server
        self._options = options
        if not self._server:
            raise Exception( "Missing connections string" )

        if not isinstance( self._server, ( list, tuple, set ) ):
            self._server = self._server.split( "," )

        self._nodes = [None] * NodeManager.Slots

        self.connection_factory = get_connection_factory( options = self._options )

        self.init_nodes()

    def connect( self, index = None ):
        """
        Given a connection index, returns a new raw redis client/connection
        instance. Index is used for master/slave setups and indicates that
        connection string should be used. In normal setups, index is 0.
        """
        if not index:
            index = random.randint( 0, len( self._server ) - 1 )
        return self.connection_factory.connect( self._server[index] )

    def get_client( self ):
        """
        Method used for obtain a raw redis client.

        This function is used by almost all cache backend
        operations for obtain a native redis client/connection
        instance.
        """
        client = None
        try:
            for index in range( len( self._server ) ):
                client = self.connect( index )
                if client:
                    break
        except:
            pass
        if not client:
            raise Exception( "All connections are not available" )
        return client

    def init_nodes( self ):
        client = self.get_client()
        if client:
            cluster_nodes = client.execute_command( "cluster", "slots" )
            print ( cluster_nodes )

    def reset_nodes( self ):
        pass


if __name__ == '__main__':
    server = [{"host":"192.168.2.237", "port":7000}, {"host":"192.168.2.237", "port":7001}, {"host":"192.168.2.237", "port":7002}, {"host":"192.168.2.237", "port":7003}, {"host":"192.168.2.237", "port":7004}, {"host":"192.168.2.237", "port":7005}]
    nodeManager = NodeManager( server, None )

