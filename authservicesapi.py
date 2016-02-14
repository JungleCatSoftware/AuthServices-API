import sys
import uuid
from database.cassandra import setupKeyspace
from flask import Flask
from flask_restful import Resource, Api, reqparse

setupKeyspace('authdb')

app = Flask(__name__)
api = Api(app)

class Root(Resource):
    def get(self):
        return { 'SUCCESS': 'true', 'From': 'authservicesapi:Root:get' }

api.add_resource(Root, '/')

if __name__ == "__main__":
    app.run(debug=True)
