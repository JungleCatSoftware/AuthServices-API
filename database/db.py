import cassandra
import datetime
import os
import time
import uuid
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from database.cassandra import CassandraCluster
from functools import wraps
from logging import getLogger

# Temp imports:
from database.cassandra import tableExists, baseline, migrateSchema

log = getLogger('gunicorn.error')


class DB:

    def createDB(keyspace, replication_class, replication_factor,
                 consistency=ConsistencyLevel.QUORUM):
        session = CassandraCluster.getSession()
        log.info('Creating Keyspace "%s"' % (keyspace,))
        session.execute(SimpleStatement(
            """
            CREATE KEYSPACE %s WITH replication
                = {'class': '%s', 'replication_factor': %s};
            """ % (keyspace, replication_class, replication_factor),
            consistency_level=consistency))

    def doMigration(session, reqid):
        log.info('Selected for migration.')

        schemaroot = os.path.join(os.getcwd(), 'schema', session.keyspace)

        log.info('Checking for schema and migrations in "%s"' % (schemaroot,))

        # Only migrate if there is a directory for the keyspace schema
        if os.path.isdir(schemaroot):
            baseline(os.path.join(schemaroot, 'baseline'),
                     session.keyspace, reqid)
            migrateSchema(os.path.join(schemaroot, 'schema_migrations'),
                          session.keyspace, reqid)
        else:
            log.info('No schema directory found for "%s"' % (session.keyspace,))

    def sessionQuery(keyspace):
        """
        Wrapper to ensure session creation for each query

        :keyspace:
            Keyspace to use for session creation.
        """
        def sessionQueryWrapper(func):
            @wraps(func)
            def func_wrapper(*args, **kwargs):
                return func(*args,
                            session=CassandraCluster.getSession(keyspace),
                            **kwargs)
            return func_wrapper
        return sessionQueryWrapper

    def requestMigration(session=None):
        """
        Request migration tasks on a keyspace, run if selected or wait if not

        :session:
            Session name to request migration task on
        """

        # Pre-fetch these prepared statements as they are used more than once
        deleteReqQuery = CassandraCluster.getPreparedStatement(
            """
            DELETE FROM schema_migration_requests
            WHERE reqid = ?
            """, keyspace=session.keyspace)

        log.info('Checking schema migration requests table')

        migrationRequestsQuery = CassandraCluster.getPreparedStatement(
            """
            SELECT * FROM schema_migration_requests
            """, keyspace=session.keyspace)
        rawMigrationRequests = session.execute(migrationRequestsQuery)\
            .current_rows

        migrationRequests = []
        staleTime = datetime.datetime.now() - datetime.timedelta(minutes=1)

        for req in rawMigrationRequests:
            # A request is "stale" if it is not in progress and it's request
            #   time is older than 1 minute (something happend while waiting),
            #   or if it is in progress but hasn't been updated in more than 1
            #   minute (something happened while it was updating), or if it is
            #   marked as "failed" (something else happened and the update was
            #   aborted).

            if (req.failed or
                    (not req.inprogress and req.reqtime < staleTime) or
                    (req.inprogress and req.lastupdate < staleTime)):

                # Delete the "stale" request (cleanup task)
                try:
                    log.info('Found stale request %s, deleting' % (req.reqid,))
                    session.execute(deleteReqQuery, (req.reqid,))
                except:
                    pass
            else:
                # Keep the record and continue
                migrationRequests.append(req)

        if len(migrationRequests) == 0:
            # No other pending/active migration requests

            reqid = uuid.uuid4()
            t = datetime.datetime.now()

            log.info('No outstanding migration requests, ' +
                     'requesting migration with ID %s' % (reqid,))

            # Nominate ourselves to run migration tasks
            requestMigrationQuery = CassandraCluster.getPreparedStatement(
                """
                INSERT INTO schema_migration_requests (reqid, reqtime,
                    inprogress, failed, lastupdate)
                VALUES (?, ?, false, false, ?)
                """, keyspace=session.keyspace)
            requestMigrationQuery.consistency_level = ConsistencyLevel.QUORUM
            session.execute(requestMigrationQuery, (reqid, t, t))

            time.sleep(2)

            log.info('Checking migration requests table to see if we ' +
                     'are selected for migration')

            # Check and see if we were selected
            migrationRequestsQuery = CassandraCluster.getPreparedStatement(
                """
                SELECT * FROM schema_migration_requests
                """, keyspace=session.keyspace)
            migrationRequests = session.execute(migrationRequestsQuery)\
                .current_rows

            # Sort by request time
            migrationRequests = sorted(migrationRequests,
                                       key=lambda x: x.reqtime)

            if (migrationRequests[0].reqid == reqid):
                # We were selected (only request or first request)

                t = datetime.datetime.now()

                # Mark ourselves as "In Progress"
                markRequestInProgressQuery = CassandraCluster\
                    .getPreparedStatement(
                        """
                        UPDATE schema_migration_requests
                        SET inprogress = true,
                        lastupdate = ?
                        WHERE reqid = ?
                        """, keyspace=session.keyspace)
                session.execute(markRequestInProgressQuery, (t, reqid))

                try:
                    # Run migration
                    DB.doMigration(session, reqid)

                    # Delete our req if successfully completed
                    session.execute(deleteReqQuery, (reqid,))

                    log.info('Migration completed successfully')
                except Exception as e:
                    log.info('Migration failed')

                    # Something went wrong, mark our req as failed
                    t = datetime.datetime.now()
                    reqFailedQuery = CassandraCluster.getPreparedStatement(
                        """
                        UPDATE schema_migration_requests
                        SET lastupdate = ?,
                        failed = true,
                        inprogress = false
                        WHERE reqid = ?
                        """, keyspace=session.keyspace)
                    session.execute(reqFailedQuery, (t, reqid))

                    raise e
            else:
                log.info('Not selected for migration (lost election)')

                # Not selected, delete our request
                session.execute(deleteReqQuery, (reqid,))

                # Wait for selected node to complete the migration
                DB.waitForMigrationCompletion(session)
        else:
            log.info('Not selected for migration (in progress)')

            # Wait for migration to complete
            DB.waitForMigrationCompletion(session)

    def setupDB(keyspace, replication_class='SimpleStrategy',
                replication_factor=1):
        try:
            DB.createDB(keyspace, replication_class, replication_factor)
        except cassandra.AlreadyExists:
            log.info('Keyspace "%s" already exists (skipping)' % (keyspace,))

        session = CassandraCluster.getSession(keyspace)

        if not tableExists(keyspace, 'schema_migrations'):
            # Create the schema_migrations table. This table stores the history
            #   of schema update scripts that have been run against the
            #   keyspace.
            try:
                log.info('Creating Schema Migrations table')
                session.execute(SimpleStatement(
                    """
                    CREATE TABLE schema_migrations (
                        scriptname text,
                        time timestamp,
                        run boolean,
                        failed boolean,
                        error text,
                        content text,
                        PRIMARY KEY (scriptname, time)
                        )
                    """, consistency_level=ConsistencyLevel.QUORUM))
            except Exception:
                log.info('Failed to create Schema Migrations table (Ignoring)')

        if not tableExists(keyspace, 'schema_migration_requests'):
            # Create schema_migration_requests table. This table is used to
            #   manage and coordinate multiple nodes requesting schema
            #   update/migrations in order to ensure only one attempts to alter
            #   schema at any time.
            try:
                log.info('Creating Schema Migration Requests table')
                session.execute(SimpleStatement(
                    """
                    CREATE TABLE schema_migration_requests (
                        reqid uuid,
                        reqtime timestamp,
                        inprogress boolean,
                        failed boolean,
                        lastupdate timestamp,
                        PRIMARY KEY (reqid)
                        )
                    """, consistency_level=ConsistencyLevel.QUORUM))
            except Exception:
                log.info('Failed to create Schema Migration Requests ' +
                         'table (Ignoring)')

        # Just to prevent race conditions on creation and read
        time.sleep(1)

        # Request migration tasks
        DB.requestMigration(session)

    def waitForMigrationCompletion(session):
        """
        Wait for a migration task running on another node to complete

        :keyspace:
            Keyspace to wait to complete migrating
        """

        migrationsRunning = True
        migrationsFailedOrStalled = False

        migrationRequestsQuery = CassandraCluster.getPreparedStatement(
            """
            SELECT * FROM schema_migration_requests
            """, keyspace=session.keyspace)

        log.info('Waiting for migrations to complete on "%s"' %
                 (session.keyspace,))

        while migrationsRunning:
            time.sleep(0.5)
            migrationRequests = session.execute(migrationRequestsQuery)\
                .current_rows

            if len(migrationRequests) == 0:
                # No Migrations running/requested, we're finished waiting
                migrationsRunning = False
                break

            # Check for stale or failed migrations
            staleTime = datetime.datetime.now() - datetime.timedelta(minutes=1)
            for req in migrationRequests:
                if (req.failed or
                        (req.inprogress and req.lastupdate < staleTime)):
                    # We found a failed or stale request (that had started),
                    #   we should re-request a migration
                    migrationsRunning = False
                    migrationsFailedOrStalled = True

        log.info('Finished waiting for migration of "%s"' % (session.keyspace,))
        if migrationsFailedOrStalled:
            log.warning('Detected failed migration of "%s", ' %
                        (session.keyspace,) +
                        'will re-request migration')
            DB.requestMigration(session)
