from cassandra import ConsistencyLevel
from database.cassandra import CassandraCluster
from functools import wraps


class AuthDB:
    keyspace = 'authdb'
    session = None

    def sessionQuery(keyspace):
        def sessionQueryWrapper(func):
            @wraps(func)
            def func_wrapper(*args, **kwargs):
                if AuthDB.session is None:
                    AuthDB.session = CassandraCluster.getSession(keyspace)
                return func(*args, **kwargs)
            return func_wrapper
        return sessionQueryWrapper

    @sessionQuery(keyspace)
    def createUser(org, username, email, parentuser,
                   consistency=ConsistencyLevel.LOCAL_QUORUM):
        createUserQuery = CassandraCluster.getPreparedStatement(
            """
            INSERT INTO users ( org, username, email, parentuser, createdate )
            VALUES ( ?, ?, ?, ?, dateof(now()) )
            """, keyspace=AuthDB.keyspace)
        createUserQuery.consistency_level = consistency
        return AuthDB.session.execute(createUserQuery,
                                      (org, username, email, parentuser))

    @sessionQuery(keyspace)
    def getOrgSetting(org, setting):
        checkOrgSetting = CassandraCluster.getPreparedStatement(
            """
            SELECT value FROM orgsettings
            WHERE org = ?
            AND setting = ?
            """, keyspace=AuthDB.keyspace)
        return AuthDB.session.execute(checkOrgSetting, (org, setting))

    @sessionQuery(keyspace)
    def getUser(org, username):
        getUserQuery = CassandraCluster.getPreparedStatement(
            """
            SELECT username, org, parentuser, createdate FROM users
            WHERE org = ?
            AND username = ?
            """, keyspace=AuthDB.keyspace)
        return AuthDB.session.execute(getUserQuery, (org, username))

    def userExists(org, username):
        return len(AuthDB.getUser(org, username).current_rows) > 0
