#!/usr/bin/env python
# -*- coding: utf-8 -*-

from rediscluster_cache.compressor.base import BaseCompressor


class IdentityCompressor(BaseCompressor):
    def compress(self, value):
        return value

    def decompress(self, value):
        return value
