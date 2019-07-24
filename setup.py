from setuptools import setup

from rediscluster_cache import __version__

description = """
Full featured rediscluster cache backend.
"""

setup(
    name = "rediscluster-cache",
    url = "https://github.com/Leon-sk/rediscluster-cache.git",
    author = "zhangwm",
    author_email = "misslittleforest@icloud.com",
    version=__version__,
    packages = [
        "rediscluster_cache",
        "rediscluster_cache.client",
        "rediscluster_cache.serializers",
        "rediscluster_cache.compressors"
    ],
    description = description.strip(),
    install_requires=[
        "hiredis>=1.0.0",
        "redis>=2.10.6",
        "msgpack>=0.6.1",
    ],
    zip_safe=False,
    include_package_data = True,
    package_data = {
        "": ["*.html"],
    },
    classifiers = [
        "Development Status :: 5 - Production/Stable",
    ],
)
