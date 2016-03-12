from functools import wraps
from database.cassandra import CassandraCluster


class DB:

    def sessionQuery(keyspace):
        """
        Wrapper to ensure session creation for each query

        :keyspace:
            Keyspace to use for session creation.
        """
        def sessionQueryWrapper(func):
            @wraps(func)
            def func_wrapper(*args, **kwargs):
                return func(*args,
                            session=CassandraCluster.getSession(keyspace),
                            **kwargs)
            return func_wrapper
        return sessionQueryWrapper
