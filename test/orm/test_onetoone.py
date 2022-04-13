from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy.orm import relationship
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.fixtures import fixture_session
from sqlalchemy.testing.schema import Column
from sqlalchemy.testing.schema import Table


class O2OTest(fixtures.MappedTest):
    @classmethod
    def define_tables(cls, metadata):
        Table(
            "jack",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("number", String(50)),
            Column("status", String(20)),
            Column("subroom", String(5)),
        )

        Table(
            "port",
            metadata,
            Column(
                "id", Integer, primary_key=True, test_needs_autoincrement=True
            ),
            Column("name", String(30)),
            Column("description", String(100)),
            Column("jack_id", Integer, ForeignKey("jack.id")),
        )

    @classmethod
    def setup_mappers(cls):
        class Jack(cls.Basic):
            pass

        class Port(cls.Basic):
            pass

    @testing.combinations(
        (True, False),
        (False, False),
        (False, True),
        argnames="_legacy_inactive_history_style, active_history",
    )
    def test_basic(self, _legacy_inactive_history_style, active_history):
        Port, port, jack, Jack = (
            self.classes.Port,
            self.tables.port,
            self.tables.jack,
            self.classes.Jack,
        )

        self.mapper_registry.map_imperatively(Port, port)
        self.mapper_registry.map_imperatively(
            Jack,
            jack,
            properties=dict(
                port=relationship(
                    Port,
                    backref="jack",
                    uselist=False,
                    active_history=active_history,
                    _legacy_inactive_history_style=(
                        _legacy_inactive_history_style
                    ),
                )
            ),
        )

        session = fixture_session()

        j = Jack(number="101")
        session.add(j)
        p = Port(name="fa0/1")
        session.add(p)

        j.port = p
        session.flush()
        jid = j.id
        pid = p.id

        j = session.get(Jack, jid)
        p = session.get(Port, pid)
        assert p.jack is not None
        assert p.jack is j
        assert j.port is not None
        p.jack = None
        assert j.port is None

        session.expunge_all()

        j = session.get(Jack, jid)
        p = session.get(Port, pid)

        j.port = None

        if not active_history and not _legacy_inactive_history_style:
            session.flush()
            self.assert_(p.jack is None)
        else:
            self.assert_(p.jack is None)
            session.flush()

        session.delete(j)
        session.flush()

    @testing.combinations(
        (True,), (False,), argnames="_legacy_inactive_history_style"
    )
    def test_simple_replace(self, _legacy_inactive_history_style):
        Port, port, jack, Jack = (
            self.classes.Port,
            self.tables.port,
            self.tables.jack,
            self.classes.Jack,
        )

        self.mapper_registry.map_imperatively(Port, port)
        self.mapper_registry.map_imperatively(
            Jack,
            jack,
            properties=dict(
                port=relationship(
                    Port,
                    uselist=False,
                    _legacy_inactive_history_style=(
                        _legacy_inactive_history_style
                    ),
                )
            ),
        )

        s = fixture_session()

        p1 = Port(name="p1")
        j1 = Jack(number="j1", port=p1)

        s.add(j1)
        s.commit()

        j1.port = Port(name="p2")
        s.commit()

        assert s.query(Port).filter_by(name="p1").one().jack_id is None

    @testing.combinations(
        (True,), (False,), argnames="_legacy_inactive_history_style"
    )
    def test_simple_del(self, _legacy_inactive_history_style):
        Port, port, jack, Jack = (
            self.classes.Port,
            self.tables.port,
            self.tables.jack,
            self.classes.Jack,
        )

        self.mapper_registry.map_imperatively(Port, port)
        self.mapper_registry.map_imperatively(
            Jack,
            jack,
            properties=dict(
                port=relationship(
                    Port,
                    uselist=False,
                    _legacy_inactive_history_style=(
                        _legacy_inactive_history_style
                    ),
                )
            ),
        )

        s = fixture_session()

        p1 = Port(name="p1")
        j1 = Jack(number="j1", port=p1)

        s.add(j1)
        s.commit()

        del j1.port
        s.commit()

        assert s.query(Port).filter_by(name="p1").one().jack_id is None
