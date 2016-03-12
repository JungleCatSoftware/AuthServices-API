from cassandra import ConsistencyLevel
from flask_restful import Resource, reqparse
from logging import getLogger
from settings import Settings
from database.authdb import AuthDB

config = Settings.getConfig()
log = getLogger('gunicorn.error')


class Users(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('username', type=str, required=True,
                            help='Username')
        parser.add_argument('org', type=str, required=True,
                            help='Org for user membership')
        parser.add_argument('email', type=str, required=True,
                            help='Email address for user')
        parser.add_argument('parentuser', type=str, required=False,
                            help='Parent user in form of user@org')
        args = parser.parse_args()
        try:
            if not AuthDB.userExists(args['org'], args['username']):
                regOpen = AuthDB.getOrgSetting(args['org'],
                                               'registrationOpen').current_rows
                if len(regOpen) == 0 or regOpen[0].value == 0:
                    return {'Message':
                            'Cannot create user "%s@%s". Organization is ' %
                            (args['username'], args['org']) +
                            'closed for registrations or does not exist.'}, 400
                else:
                    AuthDB.createUser(args['org'], args['username'],
                                      args['email'], args['parentuser'],
                                      ConsistencyLevel.QUORUM)
            else:
                return {'Message':
                        'Cannot create user "%s@%s", as it already exists.' %
                        (args['username'], args['org'])}, 400
        except Exception as e:
            log.error('Exception in Users.Post: %s' % (e,))
            return {'ServerError': 500, 'Message':
                    'There was an error fulfiling your request'}, 500
        return {'Message':
                'User "%s@%s" created.' % (args['username'], args['org'])}


class User(Resource):
    def get(self, username, org):
        """
        Retrieve basic user record information.
        """
        try:
            results = AuthDB.getUser(org, username).current_rows
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
                    'parentuser': str(results[0].parentuser),
                    'createdate': str(results[0].createdate)
                    }
            if results[0].parentuser is not None:
                user['parentuser'] = results[0].parentuser
            return user
        else:
            return {'RequestError': 400, 'Message':
                    'Request returned too many results'}, 400
