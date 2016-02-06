from cassandra.cluster import Cluster
from flask import Flask
from flask_restful import Resource, Api, reqparse
import sys, uuid

app = Flask(__name__)
api = Api(app)

cluster = Cluster(['10.10.1.25'])
session = cluster.connect('authdb')

class Root(Resource):
    def get(self):
        return { 'SUCCESS': 'true', 'From': 'authservicesapi:Root:get' }

class testing(Resource):
    def get(self):
        rows = session.execute('SELECT uid FROM testtable')
        data = { 'items': [], 'count':  0 }
        for row in rows:
            data['count']+=1
            data['items'].append({'uuid': str(row.uid)})
        return data

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('data', type=str, help='Data for new item')
        args = parser.parse_args()
        session.execute('INSERT INTO testtable (uid, data) VALUES(now(),%s)',(args['data'],))
        return {'post': 'NO UUID'}

class testingitems(Resource):
    def get(self, datauuid):
        try:
            rows = session.execute('SELECT uid, data FROM testtable WHERE uid = %s',(uuid.UUID(datauuid),))
        except:
            e = sys.exc_info()[0]
            return { 'ERROR': str(e), 'uid': datauuid }
        return { 'uuid': str(rows[0].uid), 'data': str(rows[0].data) }

    def put(self, datauuid):
        return {'put': 'WITH UUID', 'uuid': datauuid }

api.add_resource(Root, '/')
api.add_resource(testing, '/test/')
api.add_resource(testingitems, '/test/<string:datauuid>')

if __name__ == "__main__":
    app.run(debug=True)
