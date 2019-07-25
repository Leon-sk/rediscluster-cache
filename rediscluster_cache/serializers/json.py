#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

import json

from rediscluster_cache.serializers.base import BaseSerializer
from rediscluster_cache.util import force_bytes, force_text


class JSONSerializer(BaseSerializer):
    def dumps(self, value):
        return force_bytes(json.dumps(value))

    def loads(self, value):
        return json.loads(force_text(value))
