"""
Cassandra setup and management functions

Contains a class for Cluster/Session singletons and functions for
initialization and upgrade of Cassandra keyspace schema.
"""

import datetime
import os
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
                CassandraCluster.cluster = Cluster(
                    config['cassandra']['nodes'],
                    port=int(config['cassandra']['port']))
                CassandraCluster.session = {}
                CassandraCluster.preparedStmts = {}
            CassandraCluster.session[sessionLookup] = \
                CassandraCluster.cluster.connect(keyspace)
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
            CassandraCluster.preparedStmts[sessionLookup][statement] = \
                session.prepare(statement)
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
    table_count = len(session.execute(lookuptable,
                                      (keyspace, table)).current_rows)

    return True if table_count == 1 else False


def updateReq(keyspace, reqid):
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
    session.execute(reqUpdateQuery, (t, reqid))


def schemaDir(scriptstype):
    """
    Wrapper for functions that execute CQL files to accept a directory instead
    and pass the files contained in that directory to the original function.

    Additionally calls updateReq() to ensure the lastupdate time is updated for
    each file.
    """

    def schemaDir_decorator(func):
        @wraps(func)
        def func_wrapper(path, keyspace, reqid):
            if os.path.isdir(path):
                log.info('Loading %s from "%s"' % (scriptstype, path))
                contents = os.listdir(path)
                contents.sort()
                for f in contents:
                    filepath = os.path.join(path, f)
                    if os.path.isfile(filepath):
                        if f.endswith('.cql'):
                            func(filepath, keyspace)
                            updateReq(keyspace, reqid)
                        else:
                            log.info('Skipping non-CQL file "%s"' % (filepath,))
                    else:
                        log.info('Skipping non-file "%s"' % (filepath,))
            else:
                log.info('No %s found for "%s"' % (scriptstype, keyspace))
        return func_wrapper
    return schemaDir_decorator


@schemaDir('baselines')
def baseline(path, keyspace):
    """
    Execute a baseline CQL file to create tables within a keyspace. File must
    be named for the table it represents and will not be executed if the table
    already exists.
    """

    session = CassandraCluster.getSession(keyspace)
    filestart = path.rfind('/')+1
    tablename = path[filestart:-4]
    log.info('Checking table "%s"' % (tablename,))
    if not tableExists(keyspace, tablename):
        log.info('Running baseline script for "%s"' % (tablename,))
        query = open(path).read()
        try:
            session.execute(SimpleStatement(query,
                            consistency_level=ConsistencyLevel.QUORUM))
        except Exception as e:
            if tableExists(keyspace, tablename):
                # Somehow we got in this state that we shouldn't get in
                log.warning('Error creating table "%s": ' % (tablename,) +
                            'Tried to create a table that already exists!')
            else:
                raise e
    else:
        log.info('Table "%s" already exists (skipping)' % (tablename,))


@schemaDir('schema migrations')
def migrateSchema(path, keyspace):
    """
    Execute a CQL schema migration script within a keyspace. File will not be
    run if it is marked as successfully run in the schema_migrations table
    within the keyspace.
    """

    session = CassandraCluster.getSession(keyspace)

    # Get the filename part
    filestart = path.rfind('/')+1
    filename = path[filestart:]

    log.info('Checking migration script "%s"' % (filename,))

    # Get the migration history for the script
    migrationScriptHistoryQuery = CassandraCluster.getPreparedStatement("""
        SELECT * FROM schema_migrations
        WHERE scriptname = ?;
    """, keyspace=keyspace)
    migrationScriptHistory = session.execute(migrationScriptHistoryQuery,
                                             (filename,)).current_rows

    # Run if there is no history or the last execution failed
    if (len(migrationScriptHistory) == 0 or
            migrationScriptHistory[-1].failed or
            not migrationScriptHistory[-1].run):

        log.info('Running "%s" as it has not been run sucessfully' %
                 (filename,))

        content = open(path).read()
        exectime = datetime.datetime.now()

        # Insert a record of this script into the schema_migrations table and
        #   mark as not run and not failed.
        migrationScriptRunInsert = CassandraCluster.getPreparedStatement("""
            INSERT INTO schema_migrations (scriptname, time, run, failed,
                error, content)
                VALUES (?, ?, false, false, '', ?)
        """, keyspace=keyspace)
        migrationScriptRunInsert.consistency_level = ConsistencyLevel.QUORUM
        session.execute(migrationScriptRunInsert, (filename, exectime, content))

        try:
            # Run the migration script
            session.execute(SimpleStatement(content,
                            consistency_level=ConsistencyLevel.QUORUM))

            log.info('Successfully ran "%s"' % (filename,))

            # Update the script's run record as completed with success
            migrationScriptUpdateSuccess = \
                CassandraCluster.getPreparedStatement("""
                    UPDATE schema_migrations
                    SET run = true, failed = false
                    WHERE scriptname = ? AND time = ?
                """, keyspace=keyspace)
            migrationScriptUpdateSuccess.consistency_level = \
                ConsistencyLevel.QUORUM
            session.execute(migrationScriptUpdateSuccess, (filename, exectime))
        except Exception as e:
            log.info('Failed to run "%s"' % (filename,))

            # Log failure
            migrationScriptUpdateFailure = \
                CassandraCluster.getPreparedStatement("""
                    UPDATE schema_migrations
                    SET run = false, failed = true, error = ?
                    WHERE scriptname = ? AND time = ?
                """, keyspace=keyspace)
            migrationScriptUpdateFailure.consistency_level = \
                ConsistencyLevel.QUORUM
            session.execute(migrationScriptUpdateFailure,
                            (str(e), filename, exectime))

            # Pass failure upwards
            raise e
    else:
        log.info('Script "%s" has already been run on %s' %
                 (filename, migrationScriptHistory[-1].time))
