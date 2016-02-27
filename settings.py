import json
import os

from logging import getLogger
log = getLogger('gunicorn.error')


class Settings:
    sysConfigFile = '/etc/authservicesapi.conf'
    loaded = False

    config = {
        'cassandra': {
            'cluster': 'AuthServices',
            'nodes': ['127.0.0.1'],
            'port': '9042',
            'auth_keyspace': 'authdb'
        },
        'defaultorg': {
            'name': 'example.net',
            'defaultadminuser': 'admin',
            'defaultadminpass': 'admin',
            'defaultadminemail': 'admin@example.net'
        }
    }

    def getConfig():
        if not Settings.loaded:
            if os.path.isfile(Settings.sysConfigFile):
                with open(Settings.sysConfigFile, 'r') as f:

                    def mergeConfig(a, b):
                        for key in a.keys():
                            if key in b:
                                if isinstance(a[key], dict):
                                    a[key] = mergeConfig(a[key], b[key])
                                else:
                                    a[key] = b[key]
                        return a

                    Settings.config = mergeConfig(Settings.config, json.load(f))
            Settings.loaded = True
        return Settings.config
