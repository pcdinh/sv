# -*- coding: utf-8 -*-

#   __
#  /__)  __       _      _
# / (   (--  \_/ ( _ ) / _) \ /
#                     /      /

"""
Revopy Library
~~~~~~~~~~~~~~~~~~~~~
Revopy is a small Python 3 library that provides utilities to build apps using Vibora, asyncpg.

:copyright: (c) 2018 by Pham Cong Dinh.
:license: Apache 2.0, see LICENSE for more details.
"""

import os
import asyncio
import types
import errno
import asyncpg
from vibora import Vibora

__version__ = '0.0.1'


class Config:
    """Configuration loader
    :param root_path: path to which files are read relative from.
    :param defaults: an optional dictionary of default values
    """

    def __init__(self, root_path, defaults=None):
        self.dict = {}
        self.dict.update(defaults or {})
        self.root_path = root_path

    def from_py_file(self, filename, silent=False):
        """Updates the values in the config from a Python file.  This function
        behaves as if the file was imported as module with the

        :param filename: the filename of the config. This can either be an
                         absolute filename or a filename relative to the
                         root path.
        :param silent: set to ``True`` if you want silent failure for missing
                       files.
        """
        filename = os.path.join(self.root_path, filename)
        d = types.ModuleType('config')
        d.__file__ = filename
        try:
            with open(filename, mode='rb') as config_file:
                exec(compile(config_file.read(), filename, 'exec'), d.__dict__)
        except IOError as e:
            if silent and e.errno in (
                    errno.ENOENT, errno.EISDIR, errno.ENOTDIR
            ):
                return False
            e.strerror = 'Unable to load configuration file ({})'.format(e.strerror)
            raise

        for key in dir(d):
            if key.isupper():
                self.dict[key] = getattr(d, key)
        return True

    def __setitem__(self, key, value):
        pass

    def __getattr__(self, key):
        """Access an item as an attribute."""
        if key == "root_path":
            return self.root_path
        return self.dict[key]

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, dict.__repr__(self.dict))


class ConnectionManagerRegistry:
    """A store of many database connection managers for different databases"""
    def __init__(self):
        self.pools = {}

    def __getitem__(self, name):
        """Get a connection manager by name
        :param name:
        :return:
        :rtype: revopy.ds.postgresql.ConnectionManager
        """
        return self.pools.get(name, None)

    def __setitem__(self, name: str, value: asyncio.Future):
        """Add a connection manager
        :param str name:
        :param asyncio.Future value:
        :return:
        """
        self.pools[name] = value

    def __delitem__(self, name):
        del self.pools[name]
        return True

    def __contains__(self, name):
        return name in self.pools

    def pool(self, name="default") -> asyncio.Future:
        """Get an asyncpg connection pool by name
        :param name:
        :return:
        :rtype: asyncio.Future[asyncpg.pool.Pool]
        """
        return self.pools[name].pool

    async def initialize(self, app, name, db_config):
        """Initialize a database connection manager if not exists
        :param app:
        :param str name:
        :param dict db_config:
               Database connection configuration. E.x:
               {
                 "dsn": "<string>",
                 "pool": {
                   "min": <int>,
                   "max": <int>,
                   "max_inactive_connection_lifetime": <int>
                 }
               }
        :return:
        :rtype: revopy.ds.postgresql.ConnectionManager
        """
        if name not in self.pools:
            if not db_config:
                db_config = app.components.get(Config).DATABASE.get(name, None)
            pg_pool: asyncio.Future = await asyncpg.create_pool(
                db_config["dsn"],
                max_inactive_connection_lifetime=db_config["pool"]["max_inactive_connection_lifetime"],
                min_size=db_config["pool"]["min"],
                max_size=db_config["pool"]["max"],
                loop=app.loop
            )
            from revopy.ds.postgresql import ConnectionManager
            self.pools[name] = ConnectionManager(pg_pool)
        return self.pools[name]


def get_connection_manager(request, pool_name="default"):
    """Convenient function to take a supervised connection by a pool name
    :param request:
    :param pool_name:
    :return:
    """
    return request.app.db[pool_name]


async def initialize_app(app: Vibora,
                         base_path: str,
                         default_environ: str,
                         default_settings_file: str,
                         environment_settings_file: str,
                         start_db="default", event_loop=None):
    """

    :param Vibora app:
    :param str base_path:
    :param str default_environ:
    :param str default_settings_file:
    :param str environment_settings_file:
    :param str start_db:
           List of database names in a comma-separated string. E.x: default,analytics,cache. Default value: default
           See: config.settings.DATABASE
    :param event_loop:
    :return:
    """
    import logging
    environ_name = os.environ.get("ENV_NAME", default_environ)
    # Registering the config instance
    config = Config(base_path)
    config.from_py_file(default_settings_file)
    config.from_py_file(environment_settings_file.format(environ_name))
    app.components.add(config)
    # Initialize database connection pools
    if start_db is not None:
        # Ensure that "default, cache,   secret_data" is transformed into ["default", "cache", "secret_data"]
        database_names = [name.strip() for name in start_db.split(",")]
        pools = ConnectionManagerRegistry()
        for name in database_names:
            db_config = app.components.get(Config).DATABASE.get(name, None)
            if db_config is None:
                logging.error("Database pool name: %s does not exist", name)
                continue
            pg_pool: asyncio.Future = await asyncpg.create_pool(
                db_config["dsn"],
                max_inactive_connection_lifetime=db_config["pool"]["max_inactive_connection_lifetime"],
                min_size=db_config["pool"]["min"],
                max_size=db_config["pool"]["max"],
                loop=event_loop or app.loop
            )
            from revopy.ds.postgresql import ConnectionManager
            pools[name] = ConnectionManager(pg_pool)
        app.db: ConnectionManagerRegistry = pools
