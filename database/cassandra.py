"""
Cassandra setup and management functions

Contains a class for Cluster/Session singletons and functions for
initialization and upgrade of Cassandra keyspace schema.
"""

from cassandra.cluster import Cluster
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
