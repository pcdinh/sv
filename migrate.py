import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


if __name__ == '__main__':
    """
    This script is designed to migrate data

    - Dry run: Run without executing tasks (for code review ...)
    - Migrate from the latest migrated version (stored in db) to the current deployed version
    - Migrate from the specified version (user defined) to the current deployed version

    python migrate.py -s dev1 -v
    python migrate.py -s dev1 -f 2.0.0 -c 2.1.0 -d
    >> Namespace(current_version='2.1.0', dry_run=True, migrate_from='2.0.0', stage='dev1', verbose=False)
    python migrate.py -s dev1 -f 2.0.0 -c 2.1.0
    >> Namespace(current_version='2.1.0', dry_run=False, migrate_from='2.0.0', stage='dev1', verbose=False)
    """
    import os
    import asyncio
    from revopy.ds import migration
    from vibora import Vibora
    app = Vibora()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    migration_table = "_migrated_changes"

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(
        migration.run(migration_table, app, current_dir)
    )
    loop.close()

