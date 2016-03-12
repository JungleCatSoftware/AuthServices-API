from cassandra import ConsistencyLevel
from database.cassandra import CassandraCluster
from database.db import DB


class AuthDB(DB):
    """
    Class with static methods for interacting with the AuthDB using a singleton
    CassandraCluster Session object.
    """

    keyspace = 'authdb'

    @DB.sessionQuery(keyspace)
    def createUser(org, username, email, parentuser,
                   consistency=ConsistencyLevel.LOCAL_QUORUM,
                   session=None):
        """
        Create a user in the authdb.users table.

        :org:
            Name of organization
        :username:
            Name of user
        :email:
            Email address for the user
        :parentuser:
            Parent user for this user (in the form of user@org) or None
        :consistency:
            Cassandra ConsistencyLevel (default LOCAL_QUORUM)
        """
        createUserQuery = CassandraCluster.getPreparedStatement(
            """
            INSERT INTO users ( org, username, email, parentuser, createdate )
            VALUES ( ?, ?, ?, ?, dateof(now()) )
            """, keyspace=AuthDB.keyspace)
        createUserQuery.consistency_level = consistency
        return session.execute(createUserQuery,
                               (org, username, email, parentuser))

    @DB.sessionQuery(keyspace)
    def getOrgSetting(org, setting, session=None):
        """
        Get a setting/property for an organization from the authdb.orgsettings
        table.

        :org:
            Name of organization
        :setting:
            Setting/property name
        """
        checkOrgSetting = CassandraCluster.getPreparedStatement(
            """
            SELECT value FROM orgsettings
            WHERE org = ?
            AND setting = ?
            """, keyspace=AuthDB.keyspace)
        return session.execute(checkOrgSetting, (org, setting))

    @DB.sessionQuery(keyspace)
    def getUser(org, username, session=None):
        """
        Retrieve a user from the authdb.users table

        :org:
            Name of organization the user belongs to
        :username:
            Name of the user
        """
        getUserQuery = CassandraCluster.getPreparedStatement(
            """
            SELECT username, org, parentuser, createdate FROM users
            WHERE org = ?
            AND username = ?
            """, keyspace=AuthDB.keyspace)
        return session.execute(getUserQuery, (org, username))

    def userExists(org, username):
        """
        Check if a user exists in authdb.users table. Uses getUser() for user
        lookup.

        :org:
            Name of organization for user
        :username:
            Name of the user
        """
        return len(AuthDB.getUser(org, username).current_rows) > 0
