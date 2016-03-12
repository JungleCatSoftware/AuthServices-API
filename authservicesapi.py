import apis.users
from database.cassandra import setupKeyspace
from database.authdb import AuthDB
from flask import Flask
from flask_restful import Api
from logging import getLogger
from settings import Settings

log = getLogger('gunicorn.error')

config = Settings.getConfig()

setupKeyspace(config['cassandra']['auth_keyspace'])
AuthDB.createDefaultOrg(config['defaultorg']['name'],
                        config['defaultorg']['defaultadminuser'],
                        config['defaultorg']['defaultadminemail'])

app = Flask(__name__)
api = Api(app)

api.add_resource(apis.users.Users, '/users')
api.add_resource(apis.users.User, '/users/<string:username>@<string:org>')

if __name__ == "__main__":
    app.run(debug=True)
