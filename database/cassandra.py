import cassandra
from cassandra.cluster import Cluster

class CassandraCluster:
    cluster = None
    session = {}

    def getSession(keyspace=None):
        sessionLookup = '*' if keyspace is None else keyspace
        if sessionLookup not in CassandraCluster.session:
            if CassandraCluster.cluster is None:
                 CassandraCluster.cluster = Cluster(['10.10.1.25'])
            CassandraCluster.session[sessionLookup] = CassandraCluster.cluster.connect(keyspace)
        return CassandraCluster.session[sessionLookup]

    def setupKeyspace(keyspace):
        session = CassandraCluster.getSession()

        try:
            session.execute("""
                CREATE KEYSPACE %s WITH replication
                    = {'class': '%s', 'replication_factor': %s};
            """%(keyspace,'SimpleStrategy',1))
        except cassandra.AlreadyExists as e:
            # This is an okay situation
            pass

        session = CassandraCluster.getSession(keyspace)
