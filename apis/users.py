from database.cassandra import CassandraCluster
from flask_restful import Resource
from logging import getLogger
from settings import Settings

config = Settings.getConfig()
log = getLogger('gunicorn.error')


class Users(Resource):
    def post(self):
        try:
            session = CassandraCluster.getSession(
                config['cassandra']['auth_keyspace'])
        except Exception as e:
            log.error('Exception in Users.Post: %s' % (e,))
            return {'ServerError': 500, 'Message':
                    'There was an error fulfiling your request'}, 500
        return {}


class User(Resource):
    def get(self, username, org):
        try:
            session = CassandraCluster.getSession(
                config['cassandra']['auth_keyspace'])
            getUserQuery = CassandraCluster.getPreparedStatement("""
                SELECT * FROM users
                WHERE org = ?
                AND username = ?
            """, keyspace=session.keyspace)
            results = session.execute(getUserQuery,
                                      (org, username)).current_rows
        except Exception as e:
            log.error('Exception on User/get: %s' % str(e))
            return {'ServerError': 500, 'Message':
                    'There was an error fulfiling your request'}, 500
        if len(results) == 0:
            return {'Message':
                    'No user matched "%s"@"%s"' % (username, org)}, 404
        elif len(results) == 1:
            # dict(zip(n._fields, list(n)))
            user = {
                    'username': results[0].username,
                    'org': results[0].org,
                    'createdate': str(results[0].createdate)
                    }
            if results[0].parentuser is not None:
                user['parentuser'] = results[0].parentuser
            return user
        else:
            return {'RequestError': 400, 'Message':
                    'Request returned too many results'}, 400
