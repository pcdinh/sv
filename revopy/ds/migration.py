
import os
from datetime import datetime
import inspect
import logging
from importlib.machinery import SourceFileLoader

logger = logging.getLogger("revopy.migrator")


async def migrate(table_name, app, base_path, env, current_version,
                  migrate_from=None, migration_dir=None, dry_run=False, verbose=False, event_loop=None):
    """Run migration scripts
    :param str table_name:
    :param Vibora app:
    :param str base_path:
    :param str env: The environment name: dev1, dev2 ...
    :param str current_version: The current version. The data migration will always check
           the current version's migration tasks against the ones stored in the _migrated_changes table
    :param str migrate_from: Check migrated versions to ensure that nothing is missed
           E.x: We are working on 2.2.1. On prod server, we deploy 2.1.2.
           The versions that have not been deployed can be:
           + 2.1.3
           + 2.1.4
           + 2.2.0
           All data changes in those versions have been migrated already. However, we discovered a critical
           bug in 2.1.2. We delivered a bug fix in 2.1.2.1 and merged changes into 2.2.1.
           Therefore, the changes in /migration/2_1/2_1_2_1.py must be run. By default,
           the migration script will not run any changes that is older than the latest
           version that can be found in the _migrated_changes table which is 2.2.1. This option
           is designed for such a need.
    :param migration_dir:
    :param dry_run: Print all what will be executed, not executing them effectively
    :param verbose: Verbose mode
    :param event_loop:
    """
    from revopy import initialize_app
    await initialize_app(
        app, base_path, env, 'apps/config/settings.py', 'apps/config/settings_{}.py', event_loop=event_loop
    )
    if migration_dir is None:
        migration_dir = os.path.join(base_path, 'migration')

    async with app.pg.pool.acquire() as connection:
        # See:
        # http://stackoverflow.com/questions/1150032/what-is-the-benefit-of-using-set-xact-abort-on-in-a-stored-procedure
        # In SQL Server, XACT_ABORT must be set ON
        # SET XACT_ABORT ON
        # In PostgreSQL, XACT_ABORT is always ON
        # Step 1:
        # 1> If we don't need to re-scan changes deep into the history
        #    Read migration scripts for the current version
        #    Check names of changes with the _data_changes table in the current version
        #    Run the in-migrated-yet changes
        # 2> If we have to re-scan changes deep in the history
        #    Fetches all migrated changes in _data_changes table since <migrate_from>
        #    Fetches all changes stored in /migration since <migrate_from>
        #    Compare and run the in-migrated changes
        async with connection.transaction():
            await create_migration_table(table_name, connection)
            if migrate_from is None:
                # No specific version to migrate from -> I need to detect from the database
                previous_version = (await connection.fetchrow('SELECT MAX(version) AS v1 FROM %s' % table_name))["v1"]
                if previous_version is None:
                    logger.info('TODO: Scan changes from the beginning to %s', current_version)
                else:
                    if previous_version == current_version:
                        logger.info('TODO: Re-scan changes up to %s', current_version)
                    elif current_version and previous_version > current_version:
                        logger.info('------------------------------------------------------------')
                        logger.info('WARN: The database may contain changes made by newer version')
                        logger.info('WARN: Current database version: %s', previous_version)
                        logger.info('------------------------------------------------------------')
                        logger.info('TODO: Scan changes from beginning to %s', current_version)
                        q = 'SELECT COUNT(1) AS cnt FROM ' + table_name + 'WHERE version <= ' + str(current_version)
                        operations = await connection.fetchval(q)
                        if operations is None:
                            operations = 0
                        logger.info('CHECK: Migrated operation count: %s', operations)
                    else:
                        logger.info('TODO: Scan changes from %s to %s', previous_version, current_version)
            else:
                # Migrate from X to Y (X <= Y)
                if migrate_from > current_version:
                    logger.info(
                        'ERROR: The lower bound version is newer than the current version: %s-%s',
                        migrate_from, current_version
                    )
                    return
                q = 'SELECT COUNT(1) AS cnt FROM ' + table_name + \
                    'WHERE version >= ' + str(migrate_from) + ' AND version <= ' + str(current_version)
                operations = await connection.fetch_value(q)
                if operations is None:
                    operations = 0
                logger.info('CHECK: Migrated operation count: %s', operations)
                logger.info('TODO: Scan changes from %s to %s', migrate_from, current_version)
            # Perform migration tasks
            changes = load_changes(migrate_from, current_version, migration_dir)
            logger.info('MIGRATE: %s version(s)', len(changes))
            for versioned_changes in changes:
                logger.info('VERSION: %s', versioned_changes['version'])
                logger.info('  Task: %s task(s)', len(versioned_changes['changes']))
                # Executes tasks in each version
                await perform_versioned_tasks(
                    connection, versioned_changes['version'],
                    versioned_changes['changes'], dry_run, table_name
                )


async def perform_versioned_tasks(conn, version, changes, dry_run, table_name):
    """Performs migration tasks in each version
    Note: Safety over performance
    """
    for change in changes:
        logger.info('   Change: %s', change['name'])
        task = change['execute']
        recorded_version = await find_version_by_name(conn, table_name, change['name'])
        if recorded_version is not None:
            logger.info('    WARNING: Task was performed in %s', recorded_version)
            logger.info('    SKIPPED')
            continue
        # Show what will be done instead of doing it
        if dry_run is True:
            if isinstance(task, str):
                logger.info('>>> QUERY: %s', task)
            else:
                mod = inspect.getmodule(task)
                logger.info('>>> FUNCTION: %s at %s', task.__name__, mod.__file__)
        else:
            if isinstance(task, str):
                # Run the query
                await conn.execute(task)
                await track_performed_track(
                    conn, table_name, version, change['name'], change['description'], task
                )
            else:
                # Execute task
                await task(conn)
                mod = inspect.getmodule(task)
                await track_performed_track(
                    conn, table_name, version, change['name'],
                    change['description'], '%s:%s' % (mod.__file__, task.__name__)
                )


async def find_version_by_name(conn, table_name, name):
    """Finds the version that the change was performed against before"""
    return await conn.fetchval(
        f"SELECT version AS v1 FROM {table_name} WHERE name = '{name}'"
    )


async def track_performed_track(connection, table_name, version, name, description, task):
    """Records executed tasks into database for tracking"""
    await connection.execute(
        f'INSERT INTO {table_name}(version, name, description, operation, created_at)'
         'VALUES($1, $2, $3, $4, $5)',
         version, name, description, task, datetime.now()
    )


def load_changes(previous_version, current_version, path):
    """Loads changes between previous_version to the current version.
    :param previous_version: The migrated version that is loaded from database. It can be None
           If previous version is specified
           + If current version is equal to previous version, reload the script for current version and
             update the different (incremental deployment for the same version)
             ==> [previous version, current version]
           + If previous version is not equal to the current version, any script file that matches
           with the version is ignored
             ==> (previous version, current version]
    :param current_version: Version to deploy
    :param path: Path to the migration directory
    :return a list of dict (version, changes)
    """
    logger.info("Loading migration scripts at %s", path)
    ret = []
    # /path/to/<path>/<file_name:major-version-folder>/<file_name2-minor-file>
    for file_name in os.listdir(path):
        # Ignore file
        if not os.path.isdir(os.path.join(path, file_name)):
            continue
        # Sub-directory
        for file_name2 in os.listdir(path + '/' + file_name):
            if os.path.isdir(os.path.join(path, file_name, file_name2)):
                continue
            # Ignore non-py files and __init__.py
            if file_name2 == '__init__.py' or file_name2[-3:] != '.py':
                continue
            loaded_version = '.'.join([file_name, file_name2[:-3]])  # /path/1.0/1.py
            if previous_version:
                # Ignore older version
                # Note: previous_version can be current_version. In that case reload it
                if loaded_version <= previous_version and current_version != previous_version:
                    logger.info('SKIP: %s', '.'.join([file_name, file_name2[:-3]]))
                    continue
            # Ignore newer version
            if current_version and current_version < loaded_version:
                logger.info('SKIP: %s', '.'.join([file_name, file_name2[:-3]]))
                continue
            imported_mod = SourceFileLoader(file_name2[:-3], os.path.join(path, file_name, file_name2)).load_module()
            print(imported_mod)
            # version_number: changes
            # E.x: 2.0.1: changes
            if hasattr(imported_mod, 'changes'):
                ret.append({
                    'version': '%s.%s' % (file_name, file_name2[:-3]),
                    'changes': getattr(imported_mod, 'changes') or []
                })
            del imported_mod
    return ret


def parse_version(version_number):
    """Parses MAJOR.MINOR.PATCH version number
    :return a tuple(MAJOR, MINOR, PATCH). E.x: (2, 3, "1b") or (2, 3, 2)
    """
    parts = version_number.split(".", 2)
    parts[0] = int(parts[0])
    parts[1] = int(parts[1])
    try:
        parts[2] = int(parts[2])
    except Exception:
        pass
    return parts


async def create_migration_table(table_name, conn):
    q = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
          version VARCHAR(20) NOT NULL,
          name VARCHAR(50) NOT NULL,
          description VARCHAR(200),
          operation TEXT,
          created_at TIMESTAMP
        );'''
    await conn.execute(q)


async def run(migration_table, app, current_dir, event_loop=None):
    """Start migrate data

    - Dry run: Run without executing tasks (for code review ...)
    - Migrate from the latest migrated version (stored in db) to the current deployed version
    - Migrate from the specified version (user defined) to the current deployed version

    python migrate.py -s dev1 -v
    python migrate.py -s dev1 -f 2.0.0 -c 2.1.0 -d
    >> Namespace(current_version='2.1.0', dry_run=True, migrate_from='2.0.0', stage='dev1', verbose=False)
    python migrate.py -s dev1 -f 2.0.0 -c 2.1.0
    >> Namespace(current_version='2.1.0', dry_run=False, migrate_from='2.0.0', stage='dev1', verbose=False)

    Queries to test:

    INSERT INTO _migrated_changes (version, name, description, operation) VALUES (
    '2.0.0', 'A', 'B', 'C'
    );

    INSERT INTO _migrated_changes (version, name, description, operation) VALUES (
    '2.0.1', 'A', 'B', 'C'
    );
    INSERT INTO _migrated_changes (version, name, description, operation) VALUES (
    '2.0.1a' , 'A', 'B', 'C'
    );
    INSERT INTO _migrated_changes (version, name, description, operation) VALUES (
    '2.0.1b', 'A', 'B', 'C'
    );
    INSERT INTO _migrated_changes (version, name, description, operation) VALUES (
    '2.0.2', 'A', 'B', 'C'
    );

    SELECT * FROM _migrated_changes WHERE version >= '2.0.1'
    -- Expected: 2.0.1 2.0.1a 2.0.1b 2.0.2
    SELECT * FROM _migrated_changes WHERE version > '2.0.1a'
    -- Expected: 2.0.1a 2.0.1b 2.0.2

    :param migration_table:
    :param app:
    :param current_dir:
    :param event_loop:
    :return:
    """
    import argparse
    parser = argparse.ArgumentParser(description='Data migration tool', add_help=False)
    parser.add_argument('-v', action='store_true', default=False,
                        dest='verbose',
                        help='Verbose mode')
    parser.add_argument("-d", "--dry_run", dest="dry_run", action="store_true",
                        help="Dry run without actual executing", default=False)
    parser.add_argument("-f", "--migrate_from", dest="migrate_from",
                        help="The version to migrate from", type=str, default=None)
    parser.add_argument("-c", "--current_version", dest="current_version",
                        help="The version to migrate from", type=str, default=None)
    parser.add_argument("-s", "--stage", dest="stage",
                        help="Deployment stage: dev, int, staging, prod (default:%(default)s)", type=str,
                        default='dev1')
    parser.add_argument("-p", "--path", dest="path",
                        help="The path to migration files", type=str, default=None)

    cmd_args = parser.parse_args()
    await migrate(
        migration_table, app, current_dir,
        cmd_args.stage, cmd_args.current_version,
        migrate_from=cmd_args.migrate_from,
        dry_run=cmd_args.dry_run,
        migration_dir=cmd_args.path,
        verbose=cmd_args.verbose,
        event_loop=event_loop
    )
