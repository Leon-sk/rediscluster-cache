#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

import datetime
import socket
import warnings

from redis.exceptions import ConnectionError

from rediscluster_cache.exceptions import ConnectionInterrupted, CompressorError
from rediscluster_cache.nodemanager import NodeManager
from rediscluster_cache.util import DEFAULT_TIMEOUT, get_key_func, CacheKey, load_class, integer_types

# Compatibility with redis-py 2.10.6+
try:
    from redis.exceptions import TimeoutError, ResponseError
    _main_exceptions = (TimeoutError, ResponseError, ConnectionError, socket.timeout)
except ImportError:
    _main_exceptions = (ConnectionError, socket.timeout)


class DefaultClient(object):

    def __init__(self, server, params, backend):
        self._backend = backend
        self._server = server
        self._params = params

        self.reverse_key = get_key_func(params.get("REVERSE_KEY_FUNCTION") or
                                        "rediscluster_cache.util.default_reverse_key" )

        if not self._server:
            raise Exception( "Missing connections string" )

        if not isinstance(self._server, (list, tuple, set)):
            self._server = self._server.split(",")

        self._options = params.get("OPTIONS", {})

        serializer_path = self._options.get( "SERIALIZER", "rediscluster_cache.serializers.pickle.PickleSerializer" )
        serializer_cls = load_class(serializer_path)

        compressor_path = self._options.get( "COMPRESSOR", "rediscluster_cache.compressors.identity.IdentityCompressor" )
        compressor_cls = load_class(compressor_path)

        self._serializer = serializer_cls(options=self._options)
        self._compressor = compressor_cls(options=self._options)

        self.node_manager = NodeManager( self._server, self._params )

    def __contains__(self, key):
        return self.has_key(key)

    def get_client( self, key, write = True ):
        """
        Method used for obtain a raw redis client.

        This function is used by almost all cache backend
        operations for obtain a native redis client/connection
        instance.
        """
        return self.node_manager.get_node( key, write = write )

    def get_clients( self ):
        return self.node_manager.get_clients()

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None, client=None, nx=False, xx=False):
        """
        Persist a value to the cache, and set an optional expiration time.
        Also supports optional nx parameter. If set to True - will use redis setnx instead of set.
        """
        nkey = self.make_key( key, version = version )

        if not client:
            client = self.get_client( nkey, write = True )

        nvalue = self.encode(value)

        if timeout is True:
            warnings.warn("Using True as timeout value, is now deprecated.", DeprecationWarning)
            timeout = int( self._backend.default_timeout )

        if timeout == DEFAULT_TIMEOUT:
            timeout = int( self._backend.default_timeout )
        ex = datetime.timedelta( seconds = timeout )
        try:
            return client.set( nkey, nvalue, ex = ex , nx = nx, xx = xx )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client, parent=e)

    def incr_version( self, key, delta = 1, version = None ):
        """
        Adds delta to the cache version for the supplied key. Returns the
        new version.
        """

        if version is None:
            version = self._backend.version

        old_key = self.make_key(key, version)
        old_client = self.get_client( old_key, write = True )
        value = self.get( old_key, version = version, client = old_client )

        try:
            ttl = old_client.ttl( old_key )
        except _main_exceptions as e:
            raise ConnectionInterrupted( connection = old_client, parent = e )

        if value is None:
            raise ValueError("Key '%s' not found" % key)

        new_key = self.make_key( key, version = version + delta )
        new_client = self.get_client( new_key, write = True )

        self.set( new_key, value, timeout = ttl, client = new_client )
        self.delete( old_key, client = old_client )
        return version + delta

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None, client=None):
        """
        Add a value to the cache, failing if the key already exists.

        Returns ``True`` if the object was added, ``False`` if not.
        """
        return self.set(key, value, timeout, version=version, client=client, nx=True)

    def get(self, key, default=None, version=None, client=None):
        """
        Retrieve a value from the cache.

        Returns decoded value if key is found, the default if not.
        """
        key = self.make_key( key, version = version )

        if client is None:
            client = self.get_client( key, write = False )

        try:
            value = client.get(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client, parent=e)

        if value is None:
            return default

        return self.decode(value)

    def expire(self, key, timeout, version=None, client=None):
        key = self.make_key( key, version = version )

        if client is None:
            client = self.get_client( key, write = True )

        if client.exists(key):
            client.expire(key, timeout)

    def touch( self, key, timeout = DEFAULT_TIMEOUT, version = None ):
        """
        Update the key's expiry time using timeout. Return True if successful
        or False if the key does not exist.
        """
        return self.expire( key, timeout, version = version )

    def lock(self, key, version=None, timeout=None, sleep=0.1,
             blocking_timeout=None, client=None):
        key = self.make_key( key, version = version )

        if client is None:
            client = self.get_client( key, write = True )

        return client.lock(key, timeout=timeout, sleep=sleep,
                           blocking_timeout=blocking_timeout)

    def delete(self, key, version=None, prefix=None, client=None):
        """
        Remove a key from the cache.
        """
        key = self.make_key( key, version = version, prefix = prefix )

        if client is None:
            client = self.get_client( key, write = True )

        try:
            return client.delete( key )
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client, parent=e)

    def clear( self ):
        """
        Flush all cache keys.
        """
        try:
            clients = self.get_clients()
            if clients:
                for client in clients:
                    client.flushall()
        except:
            pass

    def decode(self, value):
        """
        Decode the given value.
        """
        try:
            value = int(value)
        except (ValueError, TypeError):
            try:
                value = self._compressor.decompress(value)
            except CompressorError:
                # Handle little values, chosen to be not compressed
                pass
            value = self._serializer.loads(value)
        return value

    def encode(self, value):
        """
        Encode the given value.
        """

        if isinstance(value, bool) or not isinstance(value, integer_types):
            value = self._serializer.dumps(value)
            value = self._compressor.compress(value)
            return value

        return value


    def _incr(self, key, delta=1, version=None, client=None):
        key = self.make_key( key, version = version )

        if client is None:
            client = self.get_client( key, write = True )

        try:
            try:
                # if key expired after exists check, then we get
                # key with wrong value and ttl -1.
                # use lua script for atomicity
                lua = """
                local exists = redis.call('EXISTS', KEYS[1])
                if (exists == 1) then
                    return redis.call('INCRBY', KEYS[1], ARGV[1])
                else return false end
                """
                value = client.eval(lua, 1, key, delta)
                if value is None:
                    raise ValueError("Key '%s' not found" % key)
            except ResponseError:
                # if cached value or total value is greater than 64 bit signed
                # integer.
                # elif int is encoded. so redis sees the data as string.
                # In this situations redis will throw ResponseError

                # try to keep TTL of key

                timeout = client.ttl(key)
                # returns -2 if the key does not exist
                # means, that key have expired
                if timeout == -2:
                    raise ValueError("Key '%s' not found" % key)
                value = self.get(key, version=version, client=client) + delta
                self.set(key, value, version=version, timeout=timeout,
                         client=client)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client, parent=e)

        return value

    def incr(self, key, delta=1, version=None, client=None):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        return self._incr(key=key, delta=delta, version=version, client=client)

    def decr(self, key, delta=1, version=None, client=None):
        """
        Decreace delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        return self._incr(key=key, delta=-delta, version=version,
                          client=client)

    def ttl(self, key, version=None, client=None):
        """
        Returns the remaining time to live of a key that has a timeout.
        Executes TTL redis command and return the "time-to-live" of specified key.
        If key is a non volatile key, it returns None.
        """
        key = self.make_key( key, version = version )

        if client is None:
            client = self.get_client( key, write = False )

        if not client.exists(key):
            return 0

        t = client.ttl(key)

        if t >= 0:
            return t
        elif t == -1:
            return None
        elif t == -2:
            return 0
        else:
            # Should never reach here
            return None

    def has_key(self, key, version=None, client=None):
        """
        Test if key exists.
        """
        key = self.make_key( key, version = version )

        if client is None:
            client = self.get_client( key, write = False )

        try:
            return client.exists(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=client, parent=e)

    def make_key(self, key, version=None, prefix=None):
        if isinstance( key, CacheKey ):
            return key

        if prefix is None:
            prefix = self._backend.key_prefix

        if version is None:
            version = self._backend.version

        return CacheKey( self._backend.key_func( key, prefix, version ) )

    def close( self ):
        clients = self.get_clients()
        if clients:
            for client in self.get_clients():
                if not client:
                    continue
                try:
                    if client.connection_pool:
                        client.connection_pool.disconnect()
                except:
                    pass

