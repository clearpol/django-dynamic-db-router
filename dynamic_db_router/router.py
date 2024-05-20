from contextvars import ContextVar
from functools import wraps
from uuid import uuid4
from django.db import connections
import asyncio

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
    A decorator and context manager to do queries on a given database.
    :type database: str or dict
    :param database: The database to run queries on. A string
        will route through the matching database in
        ``django.conf.settings.DATABASES``. A dictionary will set up a
        connection with the given configuration and route queries to it.
    :type read: bool, optional
    :param read: Controls whether database reads will route through
        the provided database. If ``False``, reads will route through
        the ``'default'`` database. Defaults to ``True``.
    :type write: bool, optional
    :param write: Controls whether database writes will route to
        the provided database. If ``False``, writes will route to
        the ``'default'`` database. Defaults to ``False``.
    When used as eithe a decorator or a context manager, `in_database`
    requires a single argument, which is the name of the database to
    route queries to, or a configuration dictionary for a database to
    route to.
    Usage as a context manager:
    .. code-block:: python
        from my_django_app.utils import tricky_query
        with in_database('Database_A'):
            results = tricky_query()
    Usage as a decorator:
    .. code-block:: python
        from my_django_app.models import Account
        @in_database('Database_B')
        def lowest_id_account():
            Account.objects.order_by('-id')[0]
    Used with a configuration dictionary:
    .. code-block:: python
        db_config = {'ENGINE': 'django.db.backends.sqlite3',
                     'NAME': 'path/to/mydatabase.db'}
        with in_database(db_config):
            # Run queries
    """
    def __init__(self, database: str | dict, read=True, write=False):
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
    
    def _close_connection(self):
        connections[self.unique_db_id].close()
        del connections.databases[self.unique_db_id]
    
    async def __aenter__(self):
        self.original_read_db = DB_FOR_READ_OVERRIDE.get()
        self.original_write_db = DB_FOR_WRITE_OVERRIDE.get()

        if self.read:
            DB_FOR_READ_OVERRIDE.set(self.database)
        if self.write:
            DB_FOR_WRITE_OVERRIDE.set(self.database)
        return self
            
    async def __aexit__(self, exc_type, exc, tb):
        DB_FOR_READ_OVERRIDE.set(self.original_read_db)
        DB_FOR_WRITE_OVERRIDE.set(self.original_write_db)

        if self.created_db_config:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._close_connection)

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
