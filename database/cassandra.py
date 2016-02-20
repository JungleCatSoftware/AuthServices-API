"""
Cassandra setup and management functions

Contains a class for Cluster/Session singletons and functions for
initialization and upgrade of Cassandra keyspace schema.
"""

import cassandra
import datetime
import os
import sys
import time
import uuid
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from functools import wraps
from logging import getLogger
from settings import Settings

log = getLogger('gunicorn.error')

config = Settings.getConfig()

class CassandraCluster:
    """
    Singleton class for Cassandra Cluster/Session objects

    ``Cassandracluster.getSession(keyspace)`` returns a Cassandra session object
    for the given keyspace
    """

    cluster = None
    session = {}
    preparedStmts = {}

    def getSession(keyspace=None):
        """
        Get a Cassandra session object for the given keyspace

        :keyspace:
            The keyspace for the requested session or None
        """

        sessionLookup = '*' if keyspace is None else keyspace
        if sessionLookup not in CassandraCluster.session:
            if CassandraCluster.cluster is None:
                 CassandraCluster.cluster = Cluster(config['cassandra']['nodes'],
                         port=int(config['cassandra']['port']))
                 CassandraCluster.session = {}
                 CassandraCluster.preparedStmts = {}
            CassandraCluster.session[sessionLookup] = CassandraCluster.cluster.connect(keyspace)
            CassandraCluster.preparedStmts[sessionLookup] = {}
        return CassandraCluster.session[sessionLookup]

    def getPreparedStatement(statement, keyspace=None):
        """
        Get a prepared Cassandra statement, or create it if it doesn't exist
        """

        sessionLookup = '*' if keyspace is None else keyspace
        if sessionLookup not in CassandraCluster.preparedStmts:
            CassandraCluster.preparedStmts = {}

        if statement not in CassandraCluster.preparedStmts[sessionLookup]:
            session = CassandraCluster.getSession(keyspace)
            CassandraCluster.preparedStmts[sessionLookup][statement] = session.prepare(statement)
        return CassandraCluster.preparedStmts[sessionLookup][statement]

def tableExists(keyspace, table):
    """
    Determine if the given table exists in the keyspace

    :keyspace:
        The keyspace to check for the table
    :table:
        Table to check for
    """
    if keyspace is None or table is None:
        return False

    session = CassandraCluster.getSession('system')

    lookuptable = CassandraCluster.getPreparedStatement("""
        SELECT columnfamily_name FROM schema_columnfamilies
            WHERE keyspace_name=? and columnfamily_name=?
    """, keyspace=session.keyspace)
    table_count = len(session.execute(lookuptable, (keyspace,table)).current_rows)

    return True if table_count == 1 else False

def updateReq(keyspace,reqid):
    """
    Update the lastupdate time on a reqid

    :keyspace:
        Keyspace the reqid applies to
    :reqid:
        ID of the request to be updated
    """

    session = CassandraCluster.getSession(keyspace)
    t = datetime.datetime.now()
    reqUpdateQuery = CassandraCluster.getPreparedStatement("""
        UPDATE schema_migration_requests
        SET lastupdate = ?
        WHERE reqid = ?
    """, keyspace=keyspace)
    session.execute(reqUpdateQuery, (t,reqid))


def schemaDir(scriptstype):
    """
    Wrapper for functions that execute CQL files to accept a directory instead
    and pass the files contained in that directory to the original function.

    Additionally calls updateReq() to ensure the lastupdate time is updated for
    each file.
    """

    def schemaDir_decorator(func):
        @wraps(func)
        def func_wrapper(path,keyspace,reqid):
            if os.path.isdir(path):
                log.info('Loading %s from "%s"'%(scriptstype,path))
                contents = os.listdir(path)
                contents.sort()
                for f in contents:
                    filepath = os.path.join(path, f)
                    if os.path.isfile(filepath):
                        if f.endswith('.cql'):
                            func(filepath,keyspace)
                            updateReq(keyspace,reqid)
                        else:
                            log.info('Skipping non-CQL file "%s"'%(filepath,))
                    else:
                        log.info('Skipping non-file "%s"'%(filepath,))
            else:
                log.info('No %s found for "%s"'%(scriptstype,keyspace))
        return func_wrapper
    return schemaDir_decorator

@schemaDir('baselines')
def baseline(path,keyspace):
    """
    Execute a baseline CQL file to create tables within a keyspace. File must
    be named for the table it represents and will not be executed if the table
    already exists.
    """

    session = CassandraCluster.getSession(keyspace)
    filestart = path.rfind('/')+1
    tablename = path[filestart:-4]
    log.info('Checking table "%s"'%(tablename,))
    if not tableExists(keyspace, tablename):
        log.info('Running baseline script for "%s"'%(tablename,))
        query = open(path).read()
        session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM))
    else:
        log.info('Table "%s" already exists (skipping)'%(tablename,))

@schemaDir('schema migrations')
def migrateSchema(path,keyspace):
    """
    Execute a CQL schema migration script within a keyspace. File will not be run
    if it is marked as successfully run in the schema_migrations table within the
    keyspace.
    """

    session = CassandraCluster.getSession(keyspace)

    # Get the filename part
    filestart = path.rfind('/')+1
    filename = path[filestart:]

    log.info('Checking migration script "%s"'%(filename,))

    # Get the migration history for the script
    migrationScriptHistoryQuery = CassandraCluster.getPreparedStatement("""
        SELECT * FROM schema_migrations
        WHERE scriptname = ?;
    """, keyspace=keyspace)
    migrationScriptHistory = session.execute(migrationScriptHistoryQuery, (filename,)).current_rows

    # Run if there is no history or the last execution failed
    if ( len(migrationScriptHistory) == 0 or
          migrationScriptHistory[-1].failed or not migrationScriptHistory[-1].run ):
        log.info('Running "%s" as it has not been run sucessfully'%(filename,))

        content = open(path).read()
        exectime = datetime.datetime.now()

        # Insert a record of this script into the schema_migrations table and mark as
        #   not run and not failed.
        migrationScriptRunInsert = CassandraCluster.getPreparedStatement("""
            INSERT INTO schema_migrations (scriptname, time, run, failed, error, content)
                VALUES (?, ?, false, false, '', ?)
        """, keyspace=keyspace)
        migrationScriptRunInsert.consistency_level = ConsistencyLevel.QUORUM
        session.execute(migrationScriptRunInsert, (filename,exectime,content))

        try:
            # Run the migration script
            session.execute(SimpleStatement(content, consistency_level=ConsistencyLevel.QUORUM))

            log.info('Successfully ran "%s"'%(filename,))

            # Update the script's run record as completed with success
            migrationScriptUpdateSuccess = CassandraCluster.getPreparedStatement("""
                UPDATE schema_migrations
                SET run = true, failed = false
                WHERE scriptname = ? AND time = ?
            """, keyspace=keyspace)
            migrationScriptUpdateSuccess.consistency_level = ConsistencyLevel.QUORUM
            session.execute(migrationScriptUpdateSuccess, (filename,exectime))
        except Exception as e:
            log.info('Failed to run "%s"'%(filename,))

            # Log failure
            migrationScriptUpdateFailure = CassandraCluster.getPreparedStatement("""
                UPDATE schema_migrations
                SET run = false, failed = true, error = ?
                WHERE scriptname = ? AND time = ?
            """, keyspace=keyspace)
            migrationScriptUpdateFailure.consistency_level = ConsistencyLevel.QUORUM
            session.execute(migrationScriptUpdateFailure, (str(e),filename,exectime))

            # Pass failure upwards
            raise e
    else:
        log.info('Script "%s" has already been run on %s'%(filename,migrationScriptHistory[-1].time))

def doMigration(keyspace, reqid):
    log.info('Selected for migration.')

    schemaroot = os.path.join(os.getcwd(), 'schema', keyspace)

    log.info('Checking for schema and migrations in "%s"'%(schemaroot,))

    # Only migrate if there is a directory for the keyspace schema
    if os.path.isdir(schemaroot):
        baseline(os.path.join(schemaroot, 'baseline'), keyspace, reqid)
        migrateSchema(os.path.join(schemaroot, 'schema_migrations'), keyspace, reqid)
    else:
        log.info('No schema directory found for "%s"'%(keyspace,))

def waitForMigrationCompletion(keyspace):
    """
    Wait for a migration task running on another node to complete

    :keyspace:
        Keyspace to wait to complete migrating
    """

    session = CassandraCluster.getSession(keyspace)

    migrationsRunning = True
    migrationsFailedOrStalled = False

    migrationRequestsQuery = CassandraCluster.getPreparedStatement("""
        SELECT * FROM schema_migration_requests
    """, keyspace=keyspace)

    log.info('Waiting for migrations to complete on "%s"'%(keyspace,))

    while migrationsRunning:
        time.sleep(0.5)
        migrationRequests = session.execute(migrationRequestsQuery).current_rows

        if len(migrationRequests) == 0:
            # No Migrations running/requested, we're finished waiting
            migrationsRunning = False
            break;

        # Check for stale or failed migrations
        staleTime = datetime.datetime.now() - datetime.timedelta(minutes=1)
        for req in migrationRequests:
            if ( req.failed or ( req.inprogress and req.lastupdate < staleTime ) ):
                # We found a failed or stale request (that had started),
                #   we should re-request a migration
                migrationsRunning = False
                migrationsFailedOrStalled = True

    log.info('Finished waiting for migration of "%s"'%(keyspace,))
    if migrationsFailedOrStalled:
        log.warning('Detected failed migration of "%s", will re-request migration'%(keyspace,))
        requestMigration(keyspace)


def requestMigration(keyspace):
    """
    Request migration tasks on a keyspace, run if selected or wait if not

    :keyspace:
        Keyspace naem to request migration task on
    """

    session = CassandraCluster.getSession(keyspace)

    # Pre-fetch these prepared statements as they are used more than once
    deleteReqQuery = CassandraCluster.getPreparedStatement("""
        DELETE FROM schema_migration_requests
        WHERE reqid = ?
    """, keyspace=keyspace)

    log.info('Checking schema migration requests table')

    migrationRequestsQuery = CassandraCluster.getPreparedStatement("""
        SELECT * FROM schema_migration_requests
    """, keyspace=keyspace)
    rawMigrationRequests = session.execute(migrationRequestsQuery).current_rows

    migrationRequests = []
    staleTime = datetime.datetime.now() - datetime.timedelta(minutes=1)

    for req in rawMigrationRequests:
        # A request is "stale" if it is not in progress and it's request time
        #   is older than 1 minute (something happend while waiting), or if it
        #   is in progress but hasn't been updated in more than 1 minute
        #   (something happened while it was updating), or if it is marked as
        #   "failed" (something else happened and the update was aborted).
        if ( req.failed or ( not req.inprogress and req.reqtime < staleTime ) or
             ( req.inprogress and req.lastupdate < staleTime ) ):
            # Delete the "stale" request (cleanup task)
            try:
                log.info('Found stale request %s, deleting'%(req.reqid,))
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

        log.info('No outstanding migration requests, requesting migration with ID %s'%(reqid,))

        # Nominate ourselves to run migration tasks
        requestMigrationQuery = CassandraCluster.getPreparedStatement("""
            INSERT INTO schema_migration_requests (reqid, reqtime, inprogress, failed, lastupdate)
            VALUES (?, ?, false, false, ?)
        """, keyspace=keyspace)
        requestMigrationQuery.consistency_level=ConsistencyLevel.QUORUM
        session.execute(requestMigrationQuery, (reqid, t, t))

        time.sleep(2)

        log.info('Checking migration requests table to see if we are selected for migration')

        # Check and see if we were selected
        migrationRequestsQuery = CassandraCluster.getPreparedStatement("""
            SELECT * FROM schema_migration_requests
        """, keyspace=keyspace)
        migrationRequests = session.execute(migrationRequestsQuery).current_rows

        # Sort by request time
        migrationRequests = sorted(migrationRequests, key=lambda x: x.reqtime)

        if ( migrationRequests[0].reqid == reqid ):
            # We were selected (only request or first request)

            t = datetime.datetime.now()

            # Mark ourselves as "In Progress"
            markRequestInProgressQuery = CassandraCluster.getPreparedStatement(
            """
                UPDATE schema_migration_requests
                SET inprogress = true,
                lastupdate = ?
                WHERE reqid = ?
            """, keyspace=keyspace)
            session.execute(markRequestInProgressQuery, (t, reqid))

            try:
                # Run migration
                doMigration(keyspace, reqid)

                # Delete our req if successfully completed
                session.execute(deleteReqQuery, (reqid,))

                log.info('Migration completed successfully')
            except Exception as e:
                log.info('Migration failed')

                # Something went wrong, mark our req as failed
                t = datetime.datetime.now()
                reqFailedQuery = CassandraCluster.getPreparedStatement("""
                    UPDATE schema_migration_requests
                    SET lastupdate = ?,
                    failed = true,
                    inprogress = false
                    WHERE reqid = ?
                """, keyspace=keyspace)
                session.execute(reqFailedQuery, (t,reqid))

                raise e
        else:
            log.info('Not selected for migration (lost election)')

            # Not selected, delete our request
            session.execute(deleteReqQuery, (reqid,))

            # Wait for selected node to complete the migration
            waitForMigrationCompletion(keyspace)
    else:
        log.info('Not selected for migration (in progress)')

        # Wait for migration to complete
        waitForMigrationCompletion(keyspace)

def setupKeyspace(keyspace):
    session = CassandraCluster.getSession()

    try:
        log.info('Creating Keyspace "%s"'%(keyspace,))
        # Try to create keyspace, ignore if it already exists
        session.execute(SimpleStatement("""
            CREATE KEYSPACE %s WITH replication
                = {'class': '%s', 'replication_factor': %s};
        """%(keyspace,'SimpleStrategy',1), consistency_level=ConsistencyLevel.QUORUM))
    except cassandra.AlreadyExists as e:
        log.info('Keyspace "%s" already exists (skipping)'%(keyspace,))

    session = CassandraCluster.getSession(keyspace)

    if not tableExists(keyspace, 'schema_migrations'):
        # Create the schema_migrations table. This table stores the history
        #   of schema update scripts that have been run against the keyspace.
        try:
            log.info('Creating Schema Migrations table')
            session.execute(SimpleStatement("""
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
        except Exception as e:
            log.info('Failed to create Schema Migrations table (Ignoring)')

    if not tableExists(keyspace, 'schema_migration_requests'):
        # Create schema_migration_requests table. This table is used to manage
        #   and coordinate multiple nodes requesting schema update/migrations
        #   in order to ensure only one attempts to alter schema at any time.
        try:
            log.info('Creating Schema Migration Requests table')
            session.execute(SimpleStatement("""
                CREATE TABLE schema_migration_requests (
                    reqid uuid,
                    reqtime timestamp,
                    inprogress boolean,
                    failed boolean,
                    lastupdate timestamp,
                    PRIMARY KEY (reqid)
                    )
            """, consistency_level=ConsistencyLevel.QUORUM))
        except Exception as e:
            log.info('Failed to create Schema Migration Requests table (Ignoring)')

    # Just to prevent race conditions on creation and read
    time.sleep(1)

    # Request migration tasks
    requestMigration(keyspace)
