from database.authdb import AuthDB
from flask_restful import Resource, reqparse
from logging import getLogger
from settings import Settings

config = Settings.getConfig()
log = getLogger('gunicorn.error')


class Sessions(Resource):
    def get(self, username, org):
        """
        List user's sessions
        """
        parser = reqparse.RequestParser()
        parser.add_argument('key', type=str, required=True,
                            help='Valid session key',
                            location=['headers', 'args'])
        args = parser.parse_args()

        sessionValid, sessionUser, sessionOrg = \
            AuthDB.validateSessionKey(args['key'])

        try:
            if sessionValid:
                if username == sessionUser and org == sessionOrg:
                    sessions = AuthDB.getUserSessions(org, username)
                    response = {'message': 'Found %d sessions for %s@%s' %
                                (len(sessions), username, org)}
                    response['sessions'] = list()
                    for session in sessions:
                        response['sessions'].append(
                            {'sessionid': str(session.sessionid),
                             'startdate': str(session.startdate),
                             'lastupdate': str(session.lastupdate)})
                    return response, 200
                else:
                    return {'message':
                            'You are not authorized to view this resource'}, 403
            else:
                return {'message': 'Key expired or invalid'}, 401

        except Exception as e:
            log.critical('Error in Sessions.get: %s' % (e,))

    def post(self, username, org):
        """
        Create a session for the user.
        """
        parser = reqparse.RequestParser()
        parser.add_argument('password', type=str, required=True,
                            help='PBKDF2 hash of the user\'s password using ' +
                            '"user@org" as the salt and count=10000')
        args = parser.parse_args()

        try:
            if AuthDB.userExists(org, username):
                if AuthDB.validatePassword(org, username, args['password']):
                    sessionId = AuthDB.createUserSession(org, username)
                    sessionKey = AuthDB.createUserSessionKey(org, username,
                                                             sessionId)
                    if sessionId and sessionKey:
                        return {'message': 'Session created',
                                'id': str(sessionId),
                                'key': sessionKey}
                    else:
                        return {'message': 'Failed to open session'}, 500
                else:
                    return {'message':
                            'Password authentication failed for "%s@%s".'
                            % (username, org)}, 400
            else:
                return {'message':
                        'Cannot open session for invalid user "%s@%s".'
                        % (username, org)}, 404
        except Exception as e:
            log.critical("Error in Sessions.post: %s" % (e,))


class Session(Resource):
    def get(self, username, org, sessionId=None):
        """
        List information about a user's session
        """
        parser = reqparse.RequestParser()
        parser.add_argument('key', type=str, required=True,
                            help='Valid session key',
                            location=['headers', 'args'])
        args = parser.parse_args()

        sessionValid, sessionUser, sessionOrg = \
            AuthDB.validateSessionKey(args['key'])

        if not sessionValid:
            return {'message':
                    'Invalid session key'}, 401
        elif not (sessionUser == username and sessionOrg == org):
            return {'message':
                    'You do not have permission to view this resource'}, 403

        if sessionId is None:
            session = AuthDB.getUserSessionByKey(args['key'])
            if session is None:
                return {'message': 'Server Error: Unable to get information ' +
                        'for current session'}, 500
        else:
            session = AuthDB.getUserSession(org, username, sessionId)
            if session is None:
                return {'message': 'Unable to find session %s' %
                        (str(sessionId),)}, 404

        return {'message': 'Information for session %s' % (str(sessionId),),
                'session': {
                    'username': session.username,
                    'org': session.org,
                    'sessionid': str(session.sessionid),
                    'startdate': str(session.startdate),
                    'lastupdate': str(session.lastupdate)
                }}, 200
