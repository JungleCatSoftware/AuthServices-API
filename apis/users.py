import passwordutils
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
                            help='Parent user in form of user@org',
                            default=None)
        parser.add_argument('key', type=str, required=False,
                            help='Valid session key of parentuser',
                            default=None, location=['headers', 'form', 'args'])
        args = parser.parse_args()

        parentusername, parentuserorg = '', ''
        try:
            parentusername, parentuserorg = args['parentuser'].split('@')
        except:
            pass

        if args['key'] is not None:
            sessionValid, sessionUser, sessionOrg = \
                AuthDB.validateSessionKey(args['key'])
        else:
            sessionValid, sessionUser, sessionOrg = (False, '', '')

        try:
            if not AuthDB.userExists(args['org'], args['username']):
                regOpen = AuthDB.getOrgSetting(args['org'],
                                               'registrationOpen').current_rows
                if len(regOpen) == 0 or regOpen[0].value == 0:
                    return {'Message':
                            'Cannot create user "%s@%s". Organization is ' %
                            (args['username'], args['org']) +
                            'closed for registrations or does not exist.'}, 400
                elif (args['parentuser'] is not None and
                      (len(parentusername) == 0 or len(parentuserorg) == 0 or
                       not AuthDB.userExists(parentuserorg, parentusername))):
                    return {'Message':
                            'Cannot create user "%s@%s". ' %
                            (args['username'], args['org']) +
                            'Parent user "%s" does not exist.' %
                            (args['parentuser'],)}, 400
                elif (args['parentuser'] is not None and
                        args['key'] is None):
                    return {'Message':
                            'Cannot create user "%s@%s". ' %
                            (args['username'], args['org']) +
                            'Must provide valid session key for "%s" ' %
                            (args['parentuser'],)}, 401
                elif (args['parentuser'] is not None and
                      not (sessionValid and sessionUser == parentusername and
                           sessionOrg == parentuserorg)):
                    return {'Message':
                            'Cannot create user "%s@%s". ' %
                            (args['username'], args['org']) +
                            'Session key not valid for parent user "%s".' %
                            (args['parentuser'],)}, 403
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


class RequestPasswordReset(Resource):
    def post(self, username, org):
        try:
            if AuthDB.userExists(org, username):
                resetid = AuthDB.createPasswordReset(org, username)
                if resetid:
                    # TODO: Email ResetID
                    return {'Message':
                            'Password reset for "%s"@"%s"'
                            % (username, org)}, 200
                else:
                    return {'Message':
                            'Unable to reset password for "%s"@"%s"'
                            % (username, org)}, 500
            else:
                return {'Message':
                        'Cannot reset password for invalid user "%s"@"%s"'
                        % (username, org)}, 400
        except Exception as e:
            log.error('Exception in PasswordReset.Post: %s' % (e,))
            return {'ServerError': 500, 'Message':
                    'There was an error fulfiling your request'}, 500


class CompletePasswordReset(Resource):
    def post(self, username, org):
        parser = reqparse.RequestParser()
        parser.add_argument('resetid', type=str, required=True,
                            help='ResetID of the reset request')
        parser.add_argument('password', type=str, required=True,
                            help='New password equivelent created from the ' +
                            'output of the pbkdf2 function salted with ' +
                            '"username@org" and a count of 100000')
        args = parser.parse_args()

        if AuthDB.userExists(org, username):
            if AuthDB.validatePasswordReset(org, username, args['resetid']):
                try:
                    salt = passwordutils.generateSalt()
                    passwordHash = passwordutils.hashPassword(
                        args['password'], salt, algo='argon2',
                        params={'t': 5})
                    AuthDB.setPassword(org, username, passwordHash, salt)
                except Exception as e:
                    log.error('Exeption in CompletePasswordReset Post: %s'
                              % (e,))
                    return {'message':
                            'Error changing password for "%s"@"%s"'
                            % (username, org)}, 500
                finally:
                    AuthDB.deletePasswordReset(org, username)
                return {'message': 'Password updated for "%s"@"%s".'
                        % (username, org)}, 200
            else:
                return {'message': 'Cannot change password for "%s"@"%s". '
                        % (username, org) + 'Invalid or expired resetid'}, 400
        else:
            return {'message':
                    'Cannot change password for invalid user "%s"@"%s"'
                    % (username, org)}, 400
