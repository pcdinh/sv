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


async def initialize_app(app: Vibora, base_path: str,
                         default_environ: str,
                         default_settings_file: str,
                         environment_settings_file: str,
                         start_db=True):
    """

    :param Vibora app:
    :param str base_path:
    :param str default_environ:
    :param str default_settings_file:
    :param str environment_settings_file:
    :return:
    """
    environ_name = os.environ.get("ENV_NAME", default_environ)
    # Registering the config instance
    config = Config(base_path)
    config.from_py_file(default_settings_file)
    config.from_py_file(environment_settings_file.format(environ_name))
    app.components.add(config)
    if start_db is True:
        pg_pool: asyncio.Future = await asyncpg.create_pool(
            app.components.get(Config).POSTGRESQL_DSN,
            max_inactive_connection_lifetime=config.POSTGRESQL_POOL[2],
            min_size=config.POSTGRESQL_POOL[0],
            max_size=config.POSTGRESQL_POOL[1],
            loop=app.loop
        )
        from revopy.ds.postgresql import SessionManager
        app.pg: SessionManager = SessionManager(pg_pool)
