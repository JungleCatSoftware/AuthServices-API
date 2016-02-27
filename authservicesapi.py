import apis.users
from database.cassandra import setupKeyspace, setupDefaultOrg
from flask import Flask
from flask_restful import Api
from logging import getLogger
from settings import Settings

log = getLogger('gunicorn.error')

config = Settings.getConfig()

setupKeyspace(config['cassandra']['auth_keyspace'])
setupDefaultOrg()

app = Flask(__name__)
api = Api(app)

api.add_resource(apis.users.Users, '/users')
api.add_resource(apis.users.User, '/users/<string:username>@<string:org>')

if __name__ == "__main__":
    app.run(debug=True)
