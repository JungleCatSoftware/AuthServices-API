import apis.users
from database.authdb import AuthDB
from flask import Flask
from flask_restful import Api
from logging import getLogger
from settings import Settings

log = getLogger('gunicorn.error')

config = Settings.getConfig()

log.info("Initializing database.")

AuthDB.setupDB()
AuthDB.createDefaultOrg(config['defaultorg']['name'],
                        config['defaultorg']['defaultadminuser'],
                        config['defaultorg']['defaultadminemail'])

log.info("Database initialization complete.")
log.info("Initializing Flask Application.")

app = Flask(__name__)
api = Api(app)

log.info("Adding API resources.")

api.add_resource(apis.users.Users, '/users')
api.add_resource(apis.users.User, '/users/<string:username>@<string:org>')
api.add_resource(apis.users.RequestPasswordReset,
                 '/users/<string:username>@<string:org>/requestpasswordreset')
api.add_resource(apis.users.CompletePasswordReset,
                 '/users/<string:username>@<string:org>/completepasswordreset')

log.info("Application initialization complete and ready!")

if __name__ == "__main__":
    app.run(debug=True)
