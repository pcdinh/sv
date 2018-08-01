# -*- coding: utf-8 -*-
from vibora import Vibora
from vibora.hooks import Events
from asyncpg import exceptions
import asyncpg
import asyncio
import os
import types
import errno
import logging
from contextlib import contextmanager
from revopy.ds.postgresql import SessionManager

logger = logging.getLogger("app")
app = Vibora()


class managed:
    """
    .. code:
        with managed(request.app.pq, transactional=True) as connection:
            connection.update()
            connection.insert()

        connection = request.app.pq
        with managed(connection, transactional=True, isolation='read_committed', readonly=False, deferrable=False):
            connection.update()
            connection.insert()

        connection = request.app.pq
        with managed(connection, transactional=False):
            connection.fetch_one()
            connection.fetch_all()
    """

    def __init__(self, session: SessionManager,
                 transactional: bool = False, isolation: str = 'read_committed',
                 readonly: bool = False, deferrable: bool = False):
        self.session = session
        self.transactional = transactional
        self.isolation = isolation
        self.readonly = readonly
        self.deferrable = deferrable

    async def __aenter__(self):
        # Start a connection
        try:
            await self.session.start(
                isolation='read_committed',
                readonly=False, deferrable=False
            )
            return self.session
        except BaseException as e:
            logger.exception("Error when establishing a connection")
            raise e
        # Start a transaction
        try:
            if self.transactional is True:
                self.session.start_transaction()
        except BaseException as e:
            logger.exception("Error when starting a transaction")
            await self.session.close()
            raise e

    async def __aexit__(self, exc_type, exc, tb):
        try:
            # When an exception occurs in the context scope
            if exc_type:
                if self.session.connection.is_closed():
                    await self.session.close(release=False)
                    return False  # re-raise exception without releasing connection and rollback the transaction
                # Rollback the transaction when possible
                if self.transactional is True:
                    await self.session.transaction.rollback()
                await self.session.close()
                return False  # re-raise exception
        except BaseException as e:
            logger.exception('Exceptions occur when manually releasing connection: %s', str(e))
            await self.session.close()
            return False
        # When everything is fine
        if self.session.connection.is_closed() and self.transactional is True:
            await self.session.close(release=False)
            raise exceptions.InterfaceError(
                "Unable to commit because connection was closed"
            )
        try:
            if self.transactional is True:
                await self.session.transaction.commit()
            await self.session.close()
        except BaseException as e:
            logger.exception('Exception occurs when manually releasing connection: %s', str(e))
            await self.session.close()
            return False


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


@app.handle(Events.BEFORE_SERVER_START)
async def initialize_engine(current_app: Vibora):
    # Registering the config instance.
    environ_name = os.environ.get("ENV_NAME", "dev1")
    config = Config(os.path.dirname(os.path.realpath(__file__)))
    config.from_py_file('apps/config/settings.py')
    config.from_py_file('apps/config/settings_{}.py'.format(environ_name))
    app.components.add(config)
    pg_pool: asyncio.Future = await asyncpg.create_pool(
        current_app.components.get(Config).POSTGRESQL_DSN,
        max_inactive_connection_lifetime=60,
        min_size=1,
        max_size=3,
        loop=app.loop
    )
    app.pg: SessionManager = SessionManager(pg_pool)
