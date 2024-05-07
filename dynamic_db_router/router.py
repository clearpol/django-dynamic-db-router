from contextvars import ContextVar
from functools import wraps
from uuid import uuid4
from django.db import connections

# Define context variables for read and write database settings
# These variables will maintain database preferences per context
DB_FOR_READ_OVERRIDE = ContextVar('DB_FOR_READ_OVERRIDE', default='default')
DB_FOR_WRITE_OVERRIDE = ContextVar('DB_FOR_WRITE_OVERRIDE', default='default')


class DynamicDbRouter:
    """
    A router that dynamically determines which database to perform read and write operations
    on based on the current execution context. It supports both synchronous and asynchronous code.
    """
    
    def db_for_read(self, model, **hints):
        return DB_FOR_READ_OVERRIDE.get()

    def db_for_write(self, model, **hints):
        return DB_FOR_WRITE_OVERRIDE.get()

    def allow_relation(self, *args, **kwargs):
        return True

    def allow_syncdb(self, *args, **kwargs):
        return None

    def allow_migrate(self, *args, **kwargs):
        return None

class in_database:
    """
    A context manager and decorator for setting a specific database for the duration of a block of code.
    """
    def __init__(self, database, read=True, write=False):
        self.read = read
        self.write = write
        self.database = database
        self.created_db_config = False

        # Handle database parameter either as a string (alias) or as a dict (configuration)
        if isinstance(database, str):
            self.database = database
        elif isinstance(database, dict):
            # If it's a dict, create a unique database configuration
            self.created_db_config = True
            self.unique_db_id = str(uuid4())
            connections.databases[self.unique_db_id] = database
            self.database = self.unique_db_id
        else:
            raise ValueError("database must be an identifier (str) for an existing db, "
                             "or a complete configuration (dict).")

    def __enter__(self):
        # Capture the current database settings
        self.original_read_db = DB_FOR_READ_OVERRIDE.get()
        self.original_write_db = DB_FOR_WRITE_OVERRIDE.get()

        # Override the database settings for the duration of the context
        if self.read:
            DB_FOR_READ_OVERRIDE.set(self.database)
        if self.write:
            DB_FOR_WRITE_OVERRIDE.set(self.database)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Restore the original database settings after the context.
        DB_FOR_READ_OVERRIDE.set(self.original_read_db)
        DB_FOR_WRITE_OVERRIDE.set(self.original_write_db)
        
        # Close and delete created database configuration
        if self.created_db_config:
            connections[self.unique_db_id].close()
            del connections.databases[self.unique_db_id]

    def __call__(self, querying_func):
        # Allow the object to be used as a decorator
        @wraps(querying_func)
        def inner(*args, **kwargs):
            with self:
                return querying_func(*args, **kwargs)
        return inner
