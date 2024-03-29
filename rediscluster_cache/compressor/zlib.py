#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import

import zlib

from rediscluster_cache.compressor.base import BaseCompressor
from rediscluster_cache.exceptions import CompressorError


class ZlibCompressor(BaseCompressor):
    min_length = 15
    preset = 6

    def compress(self, value):
        if len(value) > self.min_length:
            return zlib.compress(value, self.preset)
        return value

    def decompress(self, value):
        try:
            return zlib.decompress(value)
        except zlib.error as e:
            raise CompressorError(e)
