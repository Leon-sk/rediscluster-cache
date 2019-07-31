#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on 2019年7月25日

@author: leon-sk
'''
import random
from redis._compat import b, unicode, bytes, long, basestring
from rediscluster_cache.pool import get_connection_factory
from rediscluster_cache.util import crc16


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

        self._keyslot = {}

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

    def get_clients( self ):
        """
        Method used for obtain a raw redis client.

        This function is used by almost all cache backend
        operations for obtain a native redis client/connection
        instance.
        """
        clients = []
        try:
            for index in range( len( self._server ) ):
                client = self.connect( index )
                if client:
                    clients.append( client )
        except:
            pass
        if not clients:
            raise Exception( "All connections are not available" )
        return clients

    def encode( self, value ):
        """
        Return a bytestring representation of the value.
        This method is copied from Redis' connection.py:Connection.encode
        """
        if isinstance( value, bytes ):
            return value
        elif isinstance( value, ( int, long ) ):
            value = b( str( value ) )
        elif isinstance( value, float ):
            value = b( repr( value ) )
        elif not isinstance( value, basestring ):
            value = unicode( value )
        if isinstance( value, unicode ):
            # The encoding should be configurable as in connection.py:Connection.encode
            value = value.encode( 'utf-8' )
        return value

    def keyslot( self, key ):
        """
        Calculate keyslot for a given key.
        Tuned for compatibility with python 2.7.x
        """
        slot = self._keyslot.get( key )
        if not slot:
            k = self.encode( key )

            start = k.find( b"{" )

            if start > -1:
                end = k.find( b"}", start + 1 )
                if end > -1 and end != start + 1:
                    k = k[start + 1:end]

            slot = crc16( k ) % self.Slots
            self._keyslot[key] = slot
        return slot

    def cluster_slots(self):
        '''
        CLUSTER SLOTS
        Each nested result is:

        Start slot range
        End slot range
        Master for slot range represented as nested IP/Port array
        First replica of master for slot range
        Second replica
        ...continues until all replicas for this master are returned.
            '''
        cluster_nodes = None
        try:
            client = self.get_client()
            if client:
                cluster_nodes = client.execute_command( "cluster", "slots" )
                print cluster_nodes
        except:
            pass
        return cluster_nodes

    def get_connections( self, params ):
        connections = []
        if params:
            for param in params:
                host = param[0]
                port = param[1]
                connection = self.connection_factory.connect( {"host":host, "port":port} )
                if not connection:
                    continue
                self.update_server( host, port )
                connections.append( connection )
        return connections

    def update_server( self, host, port ):
        if not host or not port:
            return
        if self._server:
            for server in self._server:
                if server.get( "host" ) == host and server.get( "port" ) == port:
                    continue
                self._server.append( {"host":host, "port":port} )

    def init_nodes( self ):
        '''
        Initialize all cluster node connections
        '''
        slots = self.cluster_slots()
        if not slots:
            raise Exception( "Failed to acquire cluster slots" )
        for slot in slots:
            if not slot:
                continue
            connections = self.get_connections( slot[2:] )
            start_range = slot[0]
            end_range = slot[1]
            for num in range( start_range, end_range + 1 ):
                self._nodes[num] = connections

    def get_node( self, key, write = True ):
        slot = self.keyslot( key )
        connections = self._nodes[slot]
        if write or len( connections ) == 1:
            return connections[0]

        return random.randint( 1, len( connections ) - 1 )

    def reset_nodes( self ):
        self.init_nodes()


if __name__ == '__main__':
    server = [{"host":"192.168.2.237", "port":7000}, {"host":"192.168.2.237", "port":7001}, {"host":"192.168.2.237", "port":7002}, {"host":"192.168.2.237", "port":7003}, {"host":"192.168.2.237", "port":7004}, {"host":"192.168.2.237", "port":7005}]
    nodeManager = NodeManager( server, None )

