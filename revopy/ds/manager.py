# -*- coding: utf-8 -*-

import logging
from revopy.ds.postgresql import SessionManager
from asyncpg import exceptions

logger = logging.getLogger("revopy.ds")


class supervise:
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
        return self.session

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
