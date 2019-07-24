#!/usr/bin/python3
# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

import datetime
from decimal import Decimal
from importlib import import_module
import sys

if sys.version_info[0] < 3:
    integer_types = (int, long,)
else:
    integer_types = (int,)

# Stub class to ensure not passing in a `timeout` argument results in
# the default timeout
DEFAULT_TIMEOUT = 15

# Memcached does not accept keys longer than this.
MEMCACHE_MAX_KEY_LENGTH = 250

_PROTECTED_TYPES = ( 
    type( None ), int, float, Decimal, datetime.datetime, datetime.date, datetime.time,
 )


def default_key_func( key, key_prefix, version ):
    """
    Default function to generate keys.

    Construct the key used by all other methods. By default, prepend
    the `key_prefix'. KEY_FUNCTION can be used to specify an alternate
    function with custom key making behavior.
    """
    return '%s:%s:%s' % ( key_prefix, version, key )


def get_key_func( key_func ):
    """
    Function to decide which key function to use.

    Default to ``default_key_func``.
    """
    if key_func is not None:
        if callable( key_func ):
            return key_func
        else:
            return import_string( key_func )
    return default_key_func


def import_string( dotted_path ):
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    """
    try:
        module_path, class_name = dotted_path.rsplit( '.', 1 )
    except ValueError as err:
        raise ImportError( "%s doesn't look like a module path" % dotted_path ) from err

    module = import_module( module_path )

    try:
        return getattr( module, class_name )
    except AttributeError as err:
        raise ImportError( 'Module "%s" does not define a "%s" attribute/class' % ( 
            module_path, class_name )
        ) from err


def is_protected_type( obj ):
    """Determine if the object instance is of a protected type.

    Objects of protected types are preserved as-is when passed to
    force_text(strings_only=True).
    """
    return isinstance( obj, _PROTECTED_TYPES )


def force_text( s, encoding = 'utf-8', strings_only = False, errors = 'strict' ):
    """
    Similar to smart_text, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    # Handle the common case first for performance reasons.
    if issubclass( type( s ), str ):
        return s
    if strings_only and is_protected_type( s ):
        return s
    try:
        if isinstance( s, bytes ):
            s = str( s, encoding, errors )
        else:
            s = str( s )
    except UnicodeDecodeError as e:
        raise Exception( "error:{0},args:{1}".format( s, *e.args ) )
    return s


def smart_text( s, encoding = 'utf-8', strings_only = False, errors = 'strict' ):
    """
    Return a string representing 's'. Treat bytestrings using the 'encoding'
    codec.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    return force_text( s, encoding, strings_only, errors )


def smart_bytes( s, encoding = 'utf-8', strings_only = False, errors = 'strict' ):
    """
    Return a bytestring version of 's', encoded as specified in 'encoding'.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    return force_bytes( s, encoding, strings_only, errors )


def force_bytes( s, encoding = 'utf-8', strings_only = False, errors = 'strict' ):
    """
    Similar to smart_bytes, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    # Handle the common case first for performance reasons.
    if isinstance( s, bytes ):
        if encoding == 'utf-8':
            return s
        else:
            return s.decode( 'utf-8', errors ).encode( encoding, errors )
    if strings_only and is_protected_type( s ):
        return s
    if isinstance( s, memoryview ):
        return bytes( s )
    return str( s ).encode( encoding, errors )


class CacheKey(str):
    """
    A stub string class that we can use to check if a key was created already.
    """
    def __init__(self, key):
        self._key = key

    if sys.version_info[0] < 3:
        def __str__(self):
            return smart_bytes(self._key)

        def __unicode__(self):
            return smart_text(self._key)

    else:
        def __str__(self):
            return smart_text(self._key)

    def original_key(self):
        key = self._key.rsplit(":", 1)[1]
        return key


def load_class(path):
    """
    Loads class from path.
    """

    mod_name, klass_name = path.rsplit('.', 1)

    try:
        mod = import_module(mod_name)
    except AttributeError as e:
        raise Exception( 'Error importing {0}: "{1}"'.format( mod_name, e ) )

    try:
        klass = getattr(mod, klass_name)
    except AttributeError:
        raise Exception( 'Module "{0}" does not define a "{1}" class'.format( mod_name, klass_name ) )

    return klass

def default_reverse_key(key):
    return key.split(':', 2)[2]
