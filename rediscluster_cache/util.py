#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

import datetime
from decimal import Decimal
from importlib import import_module
import sys

# Stub class to ensure not passing in a `timeout` argument results in
# the default timeout
DEFAULT_TIMEOUT = 15

# Memcached does not accept keys longer than this.
MEMCACHE_MAX_KEY_LENGTH = 250

_PROTECTED_TYPES = ( 
    type( None ), int, float, Decimal, datetime.datetime, datetime.date, datetime.time,
 )

x_mode_m_crc16_lookup = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0
]


def _crc16_py3( data ):
    """
    """
    crc = 0
    for byte in data:
        crc = ( ( crc << 8 ) & 0xff00 ) ^ x_mode_m_crc16_lookup[( ( crc >> 8 ) & 0xff ) ^ byte]
    return crc & 0xffff


def _crc16_py2( data ):
    """
    """
    crc = 0
    for byte in data:
        crc = ( ( crc << 8 ) & 0xff00 ) ^ x_mode_m_crc16_lookup[( ( crc >> 8 ) & 0xff ) ^ ord( byte )]
    return crc & 0xffff


def default_key_func( key, key_prefix, version ):
    """
    Default function to generate keys.

    Construct the key used by all other methods. By default, prepend
    the `key_prefix'. KEY_FUNCTION can be used to specify an alternate
    function with custom key making behavior.
    """
    return "{}:{}:{}".format( key_prefix, version, key )


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
        raise ImportError( "dotted_path:{0},err:{1} doesn't look like a module path".format(dotted_path,err))

    module = import_module( module_path )

    try:
        return getattr( module, class_name )
    except AttributeError as err:
        raise ImportError( 'Module {0} does not define a {1} attribute/class,err:{2}'.format( 
            module_path, class_name, err ) )


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


if sys.version_info >= ( 3, 0, 0 ):
    crc16 = _crc16_py3
    integer_types = ( int, )
else:
    crc16 = _crc16_py2
    integer_types = ( int, long, )


class CacheKey( str ):
    """
    A stub string class that we can use to check if a key was created already.
    """

    def __init__( self, key ):
        self._key = key

    if sys.version_info[0] < 3:

        def __str__( self ):
            return smart_bytes( self._key )

        def __unicode__( self ):
            return smart_text( self._key )

    else:

        def __str__( self ):
            return smart_text( self._key )

    def original_key( self ):
        key = self._key.rsplit( ":", 1 )[1]
        return key
