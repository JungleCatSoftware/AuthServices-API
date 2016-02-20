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
from logging import getLogger
from functools import wraps

log = getLogger('gunicorn.error')

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
                 CassandraCluster.cluster = Cluster(['10.10.1.25'])
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
    session = CassandraCluster.getSession(keyspace)
    t = datetime.datetime.now()
    session.execute("""
        UPDATE schema_migration_requests
        SET lastupdate = %s
        WHERE reqid = %s
    """, (t,reqid))


def schemaDir(scriptstype):
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
    session = CassandraCluster.getSession(keyspace)
    filestart = path.rfind('/')+1
    tablename = path[filestart:-4]
    log.info('Checking table "%s"'%(tablename,))
    if not tableExists(keyspace, tablename):
        log.info('Running baseline script for "%s"'%(tablename,))
        query = open(path).read()
        session.execute(query)
    else:
        log.info('Table "%s" already exists (skipping)'%(tablename,))

@schemaDir('schema migrations')
def migrateSchema(path,keyspace):
    session = CassandraCluster.getSession(keyspace)
    filestart = path.rfind('/')+1
    filename = path[filestart:]
    log.info('Checking migration script "%s"'%(filename,))
    script_migration_data = session.execute("""
        SELECT * FROM schema_migrations
        WHERE scriptname = %s;
    """,(filename,)).current_rows
    rowcount = len(script_migration_data)
    if rowcount == 0 or script_migration_data[-1].failed:
        log.info('Running "%s" as it has not been run sucessfully'%(filename,))
        content = open(path).read()
        exectime = datetime.datetime.now()
        session.execute(SimpleStatement("""
            INSERT INTO schema_migrations (scriptname, time, run, failed, error, content)
                VALUES (%s, %s, false, false, '', %s)
        """, consistency_level=ConsistencyLevel.QUORUM), (filename,exectime,content))
        try:
            session.execute(SimpleStatement(content, consistency_level=ConsistencyLevel.QUORUM))
            session.execute(SimpleStatement("""
                UPDATE schema_migrations
                SET run = true, failed = false
                WHERE scriptname = %s AND time = %s
            """, consistency_level=ConsistencyLevel.QUORUM), (filename,exectime))
        except Exception as e:
            session.execute(SimpleStatement("""
                UPDATE schema_migrations
                SET run = false, failed = true, error = %s
                WHERE scriptname = %s AND time = %s
            """, consistency_level=ConsistencyLevel.QUORUM), (str(e),filename,exectime))
            raise e
    else:
        log.info('Script "%s" has already been run on %s'%(filename,script_migration_data[-1].time))

def doMigration(keyspace, reqid):
    log.info('Selected for migration.')
    schemaroot = os.path.join(os.getcwd(), 'schema', keyspace)
    log.info('Checking for schema and migrations in "%s"'%(schemaroot,))
    if os.path.isdir(schemaroot):
        baseline(os.path.join(schemaroot, 'baseline'), keyspace, reqid)
        migrateSchema(os.path.join(schemaroot, 'schema_migrations'), keyspace, reqid)
    else:
        log.info('No schema directory found for "%s"'%(keyspace,))

def requestMigration(keyspace):
    session = CassandraCluster.getSession(keyspace)

    log.info('Checking schema migration requests table')

    rawmigrationrequests = session.execute("""
        SELECT * FROM schema_migration_requests
    """).current_rows

    migrationrequests = []
    deletebefore = datetime.datetime.now() - datetime.timedelta(minutes=1)

    for req in rawmigrationrequests:
        if ( ( not req.inprogress and req.reqtime < deletebefore ) or
             ( req.inprogress and req.lastupdate < deletebefore ) ):
            try:
                log.info('Found stale request %s, deleting'%(req.reqid,))
                session.execute("""
                    DELETE FROM schema_migration_requests
                    WHERE reqid = %s
                """, (req.reqid,))
            except:
                pass
        else:
            migrationrequests.append(req)

    if len(migrationrequests) == 0:
        reqid = uuid.uuid4()
        t = datetime.datetime.now()
        log.info('No outstanding migration requests, requesting migration with ID %s'%(reqid,))
        session.execute(SimpleStatement("""
            INSERT INTO schema_migration_requests (reqid, reqtime, inprogress, lastupdate)
            VALUES (%s, %s, false, %s)
        """, consistency_level=ConsistencyLevel.QUORUM), (reqid, t, t))
        time.sleep(5)

        log.info('Checking migration requests table once more')
        migrationrequests = session.execute("""
            SELECT * FROM schema_migration_requests
        """).current_rows

        migrationrequests = sorted(migrationrequests, key=lambda x: x.reqtime)

        if ( len(migrationrequests) == 1 or
             migrationrequests[0].reqid == reqid ):
            session.execute("""
                UPDATE schema_migration_requests
                SET inprogress = true
                WHERE reqid = %s
            """, (reqid,))
            try:
                doMigration(keyspace, reqid)
            finally:
                session.execute("""
                    DELETE FROM schema_migration_requests
                    WHERE reqid = %s
                """, (reqid,))
        else:
            log.info('Not selected for migration')
            session.execute("""
                DELETE FROM schema_migration_requests
                WHERE reqid = %s
            """, (reqid,))
            time.sleep(10) #temporary
    else:
        log.info('Not selected for migration')
        time.sleep(10) #temporary


def setupKeyspace(keyspace):
    session = CassandraCluster.getSession()

    try:
        log.info('Creating Keyspace "%s"'%(keyspace,))
        session.execute(SimpleStatement("""
            CREATE KEYSPACE %s WITH replication
                = {'class': '%s', 'replication_factor': %s};
        """%(keyspace,'SimpleStrategy',1), consistency_level=ConsistencyLevel.QUORUM))
    except cassandra.AlreadyExists as e:
        log.info('Keyspace "%s" already exists (skipping)'%(keyspace,))
        pass

    session = CassandraCluster.getSession(keyspace)

    if not tableExists(keyspace, 'schema_migrations'):
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
        try:
            log.info('Creating Schema Migration Requests table')
            session.execute(SimpleStatement("""
                CREATE TABLE schema_migration_requests (
                    reqid uuid,
                    reqtime timestamp,
                    inprogress boolean,
                    lastupdate timestamp,
                    PRIMARY KEY (reqid)
                    )
            """, consistency_level=ConsistencyLevel.QUORUM))
        except Exception as e:
            log.info('Failed to create Schema Migration Requests table (Ignoring)')

    # Just to prevent race conditions on creation and read
    time.sleep(1)

    requestMigration(keyspace)
