from sqlalchemy.engine.interfaces import ExceptionContext


def handle_error(context: ExceptionContext) -> None:
    context.is_disconnect = True
    context.invalidate_pool_on_disconnect = False
