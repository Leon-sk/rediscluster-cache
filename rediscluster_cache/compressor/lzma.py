#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

import pylzma

from rediscluster_cache.compressor.base import BaseCompressor
from rediscluster_cache.exceptions import CompressorError


class LzmaCompressor(BaseCompressor):
    min_length = 100

    def compress(self, value):
        if len(value) > self.min_length:
            return pylzma.compress( value )
        return value

    def decompress(self, value):
        try:
            return pylzma.decompress( value )
        except Exception as ex:
            raise CompressorError( str( ex ) )
