#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

import msgpack

from rediscluster_cache.serializers.base import BaseSerializer


class MSGPackSerializer(BaseSerializer):
    def dumps(self, value):
        return msgpack.dumps(value)

    def loads(self, value):
        return msgpack.loads(value, encoding="utf-8")
