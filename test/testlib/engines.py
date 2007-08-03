from testlib import config


def testing_engine(url=None, options=None):
    """Produce an engine configured by --options with optional overrides."""
    
    from sqlalchemy import create_engine
    from testlib.testing import ExecutionContextWrapper

    url = url or config.db_url
    options = options or config.db_opts

    engine = create_engine(url, **options)

    create_context = engine.dialect.create_execution_context
    def create_exec_context(*args, **kwargs):
        return ExecutionContextWrapper(create_context(*args, **kwargs))
    engine.dialect.create_execution_context = create_exec_context
    return engine

def utf8_engine(url=None, options=None):
    """Hook for dialects or drivers that don't handle utf8 by default."""

    from sqlalchemy.engine import url as engine_url

    if config.db.name == 'mysql':
        url = url or config.db_url
        url = engine_url.make_url(url)
        url.query['charset'] = 'utf8'
        url.query['use_unicode'] = '0'
        url = str(url)

    return testing_engine(url, options)
