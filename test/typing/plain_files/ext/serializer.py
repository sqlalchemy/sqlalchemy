from typing import Any
from typing import assert_type

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.ext.serializer import dumps
from sqlalchemy.ext.serializer import loads
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker


def check_serializer_typing() -> None:
    engine = create_engine("sqlite://")
    session = scoped_session(sessionmaker(engine))

    payload = dumps({"key": "value"})

    assert_type(payload, bytes)
    assert_type(loads(payload, MetaData(), session), Any)

    with engine.connect() as connection:
        connection_session = scoped_session(sessionmaker(connection))

        assert_type(
            loads(payload, MetaData(), connection_session),
            Any,
        )
