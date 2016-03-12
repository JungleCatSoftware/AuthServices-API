from cassandra import ConsistencyLevel
from database.cassandra import CassandraCluster
from database.db import DB
from logging import getLogger

log = getLogger('gunicorn.error')


class AuthDB(DB):
    """
    Class with static methods for interacting with the AuthDB using a singleton
    CassandraCluster Session object.
    """

    keyspace = 'authdb'

    @DB.sessionQuery(keyspace)
    def createDefaultOrg(orgName, adminUser, adminEmail, session=None):
        """
        Creates and sets the default organization with an admin user. If the
        'defaultorg' global setting is not set, it will be set to the provided
        org. If it is set, the organization name in that setting will be used.
        The defaultorg will then be created if it doesn't exit. If the
        organization does not have any admins defined, the defined admin user
        will be created (if it doesn't exist) and will be set as the admin
        for the organization.

        :orgName:
            Name of the default organization
        :adminUser:
            Default admin user to create if necessary
        :adminEmail:
            Email for the default admin user
        """
        # Check global setting for DefaultOrg
        defaultOrg = AuthDB.getGlobalSetting('defaultorg').current_rows

        if len(defaultOrg) == 0:
            # DefaultOrg isn't set in database

            log.info('No DefaultOrg defined, defining as "%s"' %
                     (orgName,))

            AuthDB.setGlobalSetting('defaultorg', orgName,
                                    consistency=ConsistencyLevel.QUORUM)

            defaultOrg = AuthDB.getGlobalSetting('defaultorg').current_rows

        # Check that defined DefaultOrg exists
        org = AuthDB.getOrg(defaultOrg[0].value).current_rows

        if len(org) == 0:
            # Listed DefaultOrg doesn't exist
            log.info('DefaultOrg "%s" does not extist! ' %
                     (defaultOrg[0].value,) + 'It will be created')

            AuthDB.createOrg(defaultOrg[0].value, None)

            org = AuthDB.getOrg(defaultOrg[0].value).current_rows

        # Check that the DefaultOrg has an admin user
        orgAdmins = AuthDB.getOrgSetting(org[0].org, 'admins').current_rows

        if len(orgAdmins) == 0:
            # Org does not have admins listed
            log.info('DefaultOrg "%s" does not have an admin defined! ' %
                     (org[0].org,) + 'A default account will be added and ' +
                     'created if necessary.')

            AuthDB.setOrgSetting(org[0].org, 'admins',
                                 '%s@%s' % (adminUser, org[0].org))

            if not AuthDB.userExists(org[0].org, adminUser):
                # User doesn't exist
                log.info('Creating default admin account for "%s"' %
                         (org[0].org,))

                AuthDB.createUser(org[0].org, adminUser, adminEmail, None,
                                  consistency=ConsistencyLevel.QUORUM)

    @DB.sessionQuery(keyspace)
    def createOrg(org, parentorg,
                  consistency=ConsistencyLevel.LOCAL_QUORUM,
                  session=None):
        """
        Create an organization in the authdb.orgs table.

        :org:
            Name of the organization
        :parentorg:
            Parent organization for this organization
        :consistency:
            Cassandra consistency level. Defaults to LOCAL_QUORUM.
        """
        createOrgQuery = CassandraCluster.getPreparedStatement(
            """
            INSERT INTO orgs (org, parentorg)
            VALUES (?, ?)
            """, keyspace=session.keyspace)
        createOrgQuery.consistency_level = consistency
        session.execute(createOrgQuery, (org,))

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
            """, keyspace=session.keyspace)
        createUserQuery.consistency_level = consistency
        return session.execute(createUserQuery,
                               (org, username, email, parentuser))

    @DB.sessionQuery(keyspace)
    def getGlobalSetting(setting, session=None):
        """
        Get a setting/property for system from the authdb.globalsettings
        table.

        :setting:
            Setting/property name
        """
        getGlobalSettingQuery = CassandraCluster.getPreparedStatement(
            """
            SELECT value FROM globalsettings
            WHERE setting = ?
            """, keyspace=session.keyspace)

        return session.execute(getGlobalSettingQuery, (setting,))

    @DB.sessionQuery(keyspace)
    def getOrg(org, session=None):
        """
        Retrieve an org from the authdb.orgs table

        :org:
            Name of organization the user belongs to
        """
        getOrgQuery = CassandraCluster.getPreparedStatement(
            """
            SELECT * FROM orgs
            WHERE org = ?
            """, keyspace=session.keyspace)
        return session.execute(getOrgQuery, (org,))

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
            """, keyspace=session.keyspace)
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
            """, keyspace=session.keyspace)
        return session.execute(getUserQuery, (org, username))

    @DB.sessionQuery(keyspace)
    def setGlobalSetting(setting, value,
                         consistency=ConsistencyLevel.LOCAL_QUORUM,
                         session=None):
        """
        Set a global setting/property in the authdb.globalsettings table

        :setting:
            Setting/property name
        :value:
            Value of the setting
        :consistency:
            Cassandra consistency level. Defaults to LOCAL_QUORUM.
        """
        setGlobalSettingQuery = CassandraCluster.getPreparedStatement(
            """
            INSERT INTO globalsettings (setting, value)
            VALUES (?, ?)
            """, keyspace=session.keyspace)
        setGlobalSettingQuery.consistency_level = consistency
        session.execute(setGlobalSettingQuery, (setting, value))

    @DB.sessionQuery(keyspace)
    def setOrgSetting(org, setting, value,
                      consistency=ConsistencyLevel.LOCAL_QUORUM,
                      session=None):
        """
        Set an organization setting/property in the authdb.orgsettings table

        :org:
            Name of the organization
        :setting:
            Setting/property name
        :value:
            Value of the setting
        :consistency:
            Cassandra consistency level. Defaults to LOCAL_QUORUM.
        """
        setOrgSettingQuery = CassandraCluster.getPreparedStatement(
            """
            INSERT INTO orgsettings (org, setting, value)
            VALUES (?, ?, ?)
            """, keyspace=session.keyspace)
        setOrgSettingQuery.consistency_level = consistency
        session.execute(setOrgSettingQuery,
                        (org, setting, value))

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
