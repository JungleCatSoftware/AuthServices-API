from cassandra import ConsistencyLevel
from flask_restful import Resource, reqparse
from logging import getLogger
from settings import Settings
from database.authdb import AuthDB

config = Settings.getConfig()
log = getLogger('gunicorn.error')


class Sessions(Resource):
    def post(self, username, org):
        """
        Create a session for the user.
        """
        parser = reqparse.RequestParser()
        parser.add_argument('password', type=str, required=True,
                            help='PBKDF2 hash of the user\'s password using ' +
                            '"user@org" as the salt and count=10000')
        args = parser.parse_args()

        if AuthDB.userExists(org, username):
            if AuthDB.validatePassword(org, username, args['password']):
                return {'message': 'TEMP: VALIDATED'}
            else:
                return {'message':
                        'Password authentication failed for "%s@%s".'
                        % (username, org)}, 400
        else:
            return {'message':
                    'Cannot open session for invalid user "%s@%s".'
                    % (username, org)}, 404
