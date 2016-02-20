from database.cassandra import CassandraCluster
from flask_restful import Resource
from settings import Settings

config = Settings.getConfig()

class Users(Resource):
    def post(self):
        return {}

class User(Resource):
    def get(self,userid):
        session = CassandraCluster.getSession()
        return {}

    def put(self,userid):
        return {}
