#!/usr/bin/env python
# -*- coding: utf-8 -*-

from redis.connection import DefaultParser

from rediscluster_cache.util import load_class

SOCKET_TIMEOUT = 5
SOCKET_CONNECT_TIMEOUT = 10


class ConnectionFactory(object):

    # Store connection pool by cache backend options.
    #
    # _pools is a process-global, as otherwise _pools is cleared every time
    # ConnectionFactory is instiated, as Django creates new cache client
    # (DefaultClient) instance for every request.

    _pools = {}

    def __init__(self, options):
        pool_cls_path = options.get("CONNECTION_POOL_CLASS",
                                    "redis.connection.ConnectionPool")
        self.pool_cls = load_class( pool_cls_path )
        self.pool_cls_kwargs = options.get("CONNECTION_POOL_KWARGS", {})

        redis_client_cls_path = options.get("REDIS_CLIENT_CLASS",
                                            "redis.client.StrictRedis")
        self.redis_client_cls = load_class( redis_client_cls_path )
        self.redis_client_cls_kwargs = options.get("REDIS_CLIENT_KWARGS", {})

        self.options = options

    def make_connection_params( self, params ):
        """
        Given a main connection parameters, build a complete
        dict of connection parameters.
        """

        kwargs = {
            "parser_class": self.get_parser_cls(),
            }
        kwargs.update( params )

        password = self.options.get("PASSWORD", None)
        if password:
            kwargs["password"] = password

        socket_timeout = self.options.get( "SOCKET_TIMEOUT", SOCKET_TIMEOUT )
        if socket_timeout:
            assert isinstance(socket_timeout, (int, float)), \
                "Socket timeout should be float or integer"
            kwargs["socket_timeout"] = socket_timeout

        socket_connect_timeout = self.options.get( "SOCKET_CONNECT_TIMEOUT", SOCKET_CONNECT_TIMEOUT )
        if socket_connect_timeout:
            assert isinstance(socket_connect_timeout, (int, float)), \
                "Socket connect timeout should be float or integer"
            kwargs["socket_connect_timeout"] = socket_connect_timeout

        return kwargs

    def connect( self, params ):
        """
        Given a basic connection parameters,
        return a new connection.
        """
        params = self.make_connection_params( params )
        connection = self.get_connection(params)
        return connection

    def get_connection(self, params):
        """
        Given a now preformated params, return a
        new connection.

        The default implementation uses a cached pools
        for create new connection.
        """
        pool = self.get_or_create_connection_pool(params)
        return self.redis_client_cls(connection_pool=pool, **self.redis_client_cls_kwargs)

    def get_parser_cls(self):
        cls = self.options.get("PARSER_CLASS", None)
        if cls is None:
            return DefaultParser
        return load_class( cls )
    
    def get_connection_name(self,params):
        name = "{0}".format(params)
        if params.get("host") and params.get("port"):
            name = "{0}:{1}".format( params.get( "host" ), params.get( "port" ) )
        return name

    def get_or_create_connection_pool(self, params):
        """
        Given a connection parameters and return a new
        or cached connection pool for them.

        Reimplement this method if you want distinct
        connection pool instance caching behavior.
        """
        name = self.get_connection_name( params )
        if name not in self._pools:
            self._pools[name] = self.get_connection_pool( params )
        return self._pools[name]

    def get_connection_pool(self, params):
        """
        Given a connection parameters, return a new
        connection pool for them.

        Overwrite this method if you want a custom
        behavior on creating connection pool.
        """
        cp_params = dict(params)
        cp_params.update(self.pool_cls_kwargs)
        pool = self.pool_cls( **cp_params )

        if pool.connection_kwargs.get("password", None) is None:
            pool.connection_kwargs["password"] = params.get("password", None)
            pool.reset()

        return pool


def get_connection_factory(path=None, options=None):
    if path is None and options is not None:
        path = options.get( "REDISCLUSTER_CONNECTION_FACTORY",
                       "rediscluster_cache.pool.ConnectionFactory" )
    else:
        path = "rediscluster_cache.pool.ConnectionFactory"

    cls = load_class( path )
    return cls(options or {})
