import passwordutils
import uuid
from cassandra import ConsistencyLevel
from database.cassandra import CassandraCluster
from database.db import DB
from datetime import datetime, timedelta
from logging import getLogger
from settings import Settings

log = getLogger('gunicorn.error')


class AuthDB(DB):
    """
    Class with static methods for interacting with the AuthDB using a singleton
    CassandraCluster Session object.
    """

    config = Settings.getConfig()
    keyspace = config['cassandra']['auth_keyspace']

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
    def createPasswordReset(org, username,
                            consistency=ConsistencyLevel.LOCAL_QUORUM,
                            session=None):
        """
        Create a password reset in authdb.userpasswordresets table.

        :org:
            Name of organization
        :username:
            Name of user
        """
        createPasswordResetQuery = CassandraCluster.getPreparedStatement(
            """
            INSERT INTO userpasswordresets ( org, username, requestdate,
                                             resetid )
            VALUES ( ?, ?, dateof(now()), ? )
            """, keyspace=session.keyspace)
        createPasswordResetQuery.consistency_level = consistency

        resetid = uuid.uuid4()

        try:
            session.execute(createPasswordResetQuery,
                            (org, username, resetid))
            return resetid
        except Exception as e:
            log.error("Caught exception in AuthDB.createPasswordReset: %s"
                      % (e,))
            return False

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
    def deletePasswordReset(org, username,
                            consistency=ConsistencyLevel.LOCAL_QUORUM,
                            session=None):
        """
        Delete/remove a password reset request for a user

        :org:
            Name of organization the user belongs to
        :username:
            Name of user
        :consistency:
            Cassandra ConsistencyLevel (default LOCAL_QUORUM)
        """
        deletePasswordResetQuery = CassandraCluster.getPreparedStatement(
            """
            DELETE FROM userpasswordresets
            WHERE org = ?
            AND username = ?
            """, keyspace=session.keyspace)
        deletePasswordResetQuery.consistency_level = consistency
        session.execute(deletePasswordResetQuery,
                        (org, username))

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
    def getPasswordReset(org, username, session=None):
        """
        Retrieve a password reset request from the authdb.userpasswordresets
        table

        :org:
            Name of organization the user belongs to
        :username:
            Name of the user
        """
        getPasswordResetQuery = CassandraCluster.getPreparedStatement(
            """
            SELECT username, org, requestdate, resetid FROM userpasswordresets
            WHERE org = ?
            AND username = ?
            """, keyspace=session.keyspace)
        return session.execute(getPasswordResetQuery, (org, username))

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
    def getUserHash(org, username, session=None):
        """
        Retrieve a user's password hash from the authdb.users table

        :org:
            Name of organization the user belongs to
        :username:
            Name of the user
        """
        getUserHashQuery = CassandraCluster.getPreparedStatement(
            """
            SELECT hash FROM users
            WHERE org = ?
            AND username = ?
            """, keyspace=session.keyspace)
        res = session.execute(getUserHashQuery, (org, username)).current_rows
        if len(res) > 0:
            return res[0].hash
        else:
            return None

    @DB.sessionQuery(keyspace)
    def getUserSalt(org, username, session=None):
        """
        Retrieve a user's salt from the authdb.users table

        :org:
            Name of organization the user belongs to
        :username:
            Name of the user
        """
        getUserSaltQuery = CassandraCluster.getPreparedStatement(
            """
            SELECT salt FROM users
            WHERE org = ?
            AND username = ?
            """, keyspace=session.keyspace)
        res = session.execute(getUserSaltQuery, (org, username)).current_rows
        if len(res) > 0:
            return res[0].salt
        else:
            return None

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

    @DB.sessionQuery(keyspace)
    def setPassword(org, username, passwordHash, salt,
                    consistency=ConsistencyLevel.LOCAL_QUORUM,
                    session=None):
        """
        Update/set user's password with given hash and salt

        :org:
            Name of org the user is in
        :username:
            Name of user
        :passwordHash:
            The Argon2 hash of the salted password
        :salt:
            Salt used to generate the hash
        :consistency:
            Cassandra consistency level. Defaults to LOCAL_QUORUM.
        """
        setPasswordQuery = CassandraCluster.getPreparedStatement(
            """
            UPDATE users SET
            hash = ?,
            salt = ?
            WHERE org = ?
            AND username = ?
            """, keyspace=session.keyspace)
        setPasswordQuery.consistency_level = consistency
        session.execute(setPasswordQuery, (passwordHash, salt, org, username))

    def setupDB(replication_class='SimpleStrategy', replication_factor=1):
        DB.setupDB(AuthDB.keyspace, replication_class=replication_class,
                   replication_factor=replication_factor)

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

    def validatePassword(org, username, password):
        """
        Compare the given password against the hashed version for the user

        :org:
            Organization of the user to check
        :username:
            Name of the user to check
        :password:
            Raw password of the user, without salt
        """
        salt = AuthDB.getUserSalt(org, username)
        if salt is not None:
            computedHash = passwordutils.hashPassword(
                password, salt, algo='argon2', params={'t': 5})
            storedHash = AuthDB.getUserHash(org, username)
            if computedHash == storedHash:
                return True
        return False

    def validatePasswordReset(org, username, resetid):
        """
        Verify a password reset UUID against the record for that user. Returns
        True if the UUID matches the user's record and the record's date is less
        than 7 days old, returns False otherwise.

        :org:
            Organization of the user to check
        :username:
            Name of the user to check
        :resetid:
            UUID of the password reset to check
        """
        resetRecord = AuthDB.getPasswordReset(org, username)
        if (len(resetRecord.current_rows) > 0 and
                (resetRecord[0].requestdate + timedelta(days=7)) >
                datetime.now() and
                str(resetRecord[0].resetid) == resetid):
            return True
        else:
            return False
