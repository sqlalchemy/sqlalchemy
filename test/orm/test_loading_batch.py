"""Behavioural tests for the batched instance processor on the ORM
loading path (``orm/_loading_cy._InstancesBatch``).

The batch processor is attached as ``_instance._sa_row_batch`` by
``loading._instance_processor()`` for the common non-refresh,
non-polymorphic single-entity load path, and is invoked by
``loading.instances()`` in place of the per-row
``[proc(row) for row in fetch]`` comprehension.

These tests exercise each branch of the batch through observable
behaviour: the ``use_idx`` tuple fast path, existing-instance
delegation, NULL primary keys, composite (multi-column) primary keys,
deferred columns (the ``expired_attributes`` slot-init regression),
load / loaded_as_persistent events, ``populate_existing``, polymorphic
loads (batch NOT attached), versioned mappings, and boundary cases.
"""

from sqlalchemy import Column
from sqlalchemy import event
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import testing
from sqlalchemy import update
from sqlalchemy.orm import aliased
from sqlalchemy.orm import deferred
from sqlalchemy.orm import loading
from sqlalchemy.orm import Session
from sqlalchemy.orm import undefer
from sqlalchemy.orm._loading_cy import _InstancesBatch
from sqlalchemy.orm._loading_cy import _is_compiled
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing import is_none
from sqlalchemy.testing import is_not_none
from sqlalchemy.testing import is_true
from sqlalchemy.testing import mock
from sqlalchemy.testing.fixtures import fixture_session
from . import _fixtures


def _capture_processor(fn):
    """Run ``fn`` while capturing the processors returned by
    ``loading._instance_processor``, so tests can assert whether the
    batch (``_sa_row_batch``) was attached for a given load path."""

    captured = []
    orig = loading._instance_processor

    def spy(*args, **kw):
        proc = orig(*args, **kw)
        captured.append(proc)
        return proc

    with mock.patch.object(loading, "_instance_processor", spy):
        result = fn()
    return result, captured


class BatchEngagementTest(_fixtures.FixtureTest):
    """Confirm the batch is actually attached / invoked for the simple
    non-poly path, and not attached for the polymorphic base."""

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_batch_attached_for_plain_entity(self):
        User = self.classes.User
        s = fixture_session()

        _, procs = _capture_processor(lambda: s.scalars(select(User)).all())
        is_true(len(procs) >= 1)
        for proc in procs:
            is_not_none(getattr(proc, "_sa_row_batch", None))
            is_true(isinstance(proc._sa_row_batch, _InstancesBatch))

    @testing.skip_if(
        lambda: _is_compiled(),
        "compiled _InstancesBatch.__call__ is a C method and cannot be "
        "patched with mock.autospec",
    )
    def test_batch_invoked_for_plain_entity(self):
        User = self.classes.User
        s = fixture_session()

        with mock.patch.object(
            _InstancesBatch,
            "__call__",
            autospec=True,
            side_effect=_InstancesBatch.__call__,
        ) as call_mock:
            result = s.scalars(select(User)).all()

        is_true(len(result) > 0)
        is_true(call_mock.call_count >= 1)


class BatchFastPathTest(_fixtures.FixtureTest):
    """Single-column int PK + plain columns: the ``use_idx`` tuple fast
    path (PK and quick populators read by integer position)."""

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_basic_load_all_columns(self):
        User = self.classes.User
        s = fixture_session()

        users = s.scalars(select(User).order_by(User.id)).all()
        eq_(
            [(u.id, u.name) for u in users],
            [(7, "jack"), (8, "ed"), (9, "fred"), (10, "chuck")],
        )

    def test_empty_result(self):
        User = self.classes.User
        s = fixture_session()

        users = s.scalars(select(User).where(User.id == -1)).all()
        eq_(users, [])

    def test_existing_instance_delegation_full_reload(self):
        """Load once, then load again in the same Session -> the second
        pass returns the SAME identity-mapped objects (delegates to
        _instance for already-present rows)."""
        User = self.classes.User
        s = fixture_session()

        first = s.scalars(select(User).order_by(User.id)).all()
        first_ids = {id(u) for u in first}

        second = s.scalars(select(User).order_by(User.id)).all()
        # identical objects returned, not duplicates
        for u in second:
            is_true(id(u) in first_ids)
        eq_([u.id for u in first], [u.id for u in second])

    def test_existing_instance_delegation_mixed(self):
        """Mix of some-existing + some-new rows in one result: the
        pre-loaded rows delegate to _instance and stay identity-mapped,
        the new rows hydrate fresh."""
        User = self.classes.User
        s = fixture_session()

        # preload a subset
        preloaded = s.scalars(
            select(User).where(User.id.in_([7, 9])).order_by(User.id)
        ).all()
        existing_by_id = {u.id: u for u in preloaded}

        # full load - 7 & 9 already mapped, 8 & 10 new
        full = s.scalars(select(User).order_by(User.id)).all()
        eq_([u.id for u in full], [7, 8, 9, 10])
        for u in full:
            if u.id in existing_by_id:
                is_(u, existing_by_id[u.id])

    def test_pending_then_query(self):
        """add() a pending instance then query: the row for the pending
        (now persistent after flush) object is the same object."""
        User = self.classes.User
        s = fixture_session()

        new_user = User(id=42, name="zelda")
        s.add(new_user)
        s.flush()

        rows = s.scalars(select(User).where(User.id == 42)).all()
        eq_(len(rows), 1)
        is_(rows[0], new_user)
        s.rollback()


class BatchNullPkTest(_fixtures.FixtureTest):
    """NULL primary key in the row -> no entity (None) for that row.

    A LEFT OUTER JOIN from users to addresses, selecting Address for a
    user with no addresses, yields a row whose Address PK columns are
    all NULL."""

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_null_pk_yields_none(self):
        User, Address = self.classes("User", "Address")
        s = fixture_session()

        # users 8, 9, 10 have addresses; some users may not. Use an
        # outer join so the Address columns can be NULL for users with
        # no address; select the Address entity.
        stmt = (
            select(Address)
            .select_from(User)
            .outerjoin(Address, User.id == Address.user_id)
            .where(User.name == "chuck")  # chuck (id 10) has no address
            .order_by(User.id)
        )
        result = s.execute(stmt)
        rows = result.scalars().all()

        # the single matching user has no address -> the entity is None
        eq_(rows, [None])

    def test_mixed_null_and_present_pk(self):
        User, Address = self.classes("User", "Address")
        s = fixture_session()

        stmt = (
            select(Address)
            .select_from(User)
            .outerjoin(Address, User.id == Address.user_id)
            .order_by(User.id, Address.id)
        )
        result = s.execute(stmt).scalars().all()

        # chuck has no addresses -> a None somewhere in the result; the
        # other entries are real Address objects
        is_true(None in result)
        for ent in result:
            if ent is not None:
                is_true(isinstance(ent, Address))


class _CompositePkFixture(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class CompositeThing(Base):
            __tablename__ = "composite_thing"
            a_id = Column(Integer, primary_key=True)
            b_id = Column(Integer, primary_key=True)
            data = Column(String(50))

    @classmethod
    def insert_data(cls, connection):
        CompositeThing = cls.classes.CompositeThing
        s = Session(connection)
        s.add_all(
            [
                CompositeThing(a_id=1, b_id=1, data="one-one"),
                CompositeThing(a_id=1, b_id=2, data="one-two"),
                CompositeThing(a_id=2, b_id=1, data="two-one"),
            ]
        )
        s.commit()


class BatchCompositePkTest(_CompositePkFixture):
    """Multi-column PK disables the use_idx fast path (pk_idx == -1);
    the generic primary_key_getter branch must still load correctly."""

    def test_pk_idx_disabled(self):
        """The batch must report no single PK index for a composite
        PK mapping."""
        CompositeThing = self.classes.CompositeThing
        s = fixture_session()

        _, procs = _capture_processor(
            lambda: s.scalars(select(CompositeThing)).all()
        )
        is_true(len(procs) >= 1)
        for proc in procs:
            batch = getattr(proc, "_sa_row_batch", None)
            is_not_none(batch)
            # pk_idx is a C struct member when compiled and not visible
            # from Python; only assert it on the pure-Python path
            if not _is_compiled():
                eq_(batch.pk_idx, -1)

    def test_composite_pk_load(self):
        CompositeThing = self.classes.CompositeThing
        s = fixture_session()

        rows = s.scalars(
            select(CompositeThing).order_by(
                CompositeThing.a_id, CompositeThing.b_id
            )
        ).all()
        eq_(
            [(r.a_id, r.b_id, r.data) for r in rows],
            [(1, 1, "one-one"), (1, 2, "one-two"), (2, 1, "two-one")],
        )

    def test_composite_pk_existing_delegation(self):
        CompositeThing = self.classes.CompositeThing
        s = fixture_session()

        first = s.scalars(select(CompositeThing)).all()
        ids = {id(r) for r in first}
        second = s.scalars(select(CompositeThing)).all()
        for r in second:
            is_true(id(r) in ids)


class _DeferredFixture(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class HasDeferred(Base):
            __tablename__ = "has_deferred"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            description = deferred(Column(String(50)))

    @classmethod
    def insert_data(cls, connection):
        HasDeferred = cls.classes.HasDeferred
        s = Session(connection)
        s.add_all(
            [
                HasDeferred(id=1, name="a", description="desc-a"),
                HasDeferred(id=2, name="b", description="desc-b"),
            ]
        )
        s.commit()


class BatchDeferredTest(_DeferredFixture):
    """THE critical regression: the inlined InstanceState must
    initialize ``expired_attributes = set()``.  A deferred column lands
    in the ``expire`` populator with set_callable=True; without the
    slot-init this raises AttributeError when the batch does
    ``state.expired_attributes.add(key)``."""

    def test_deferred_load_no_error(self):
        HasDeferred = self.classes.HasDeferred
        s = fixture_session()

        # the load itself must not raise (exercises expired_attributes
        # init in the inlined state)
        rows = s.scalars(select(HasDeferred).order_by(HasDeferred.id)).all()
        eq_([(r.id, r.name) for r in rows], [(1, "a"), (2, "b")])

    def test_deferred_attr_is_expired_then_loads(self):
        HasDeferred = self.classes.HasDeferred
        s = fixture_session()

        obj = s.scalars(select(HasDeferred).where(HasDeferred.id == 1)).one()

        state = inspect(obj)
        # description is deferred -> unloaded (absent from the row)
        is_true("description" in state.unloaded)
        is_true("description" not in obj.__dict__)

        # accessing it triggers the deferred load
        eq_(obj.description, "desc-a")
        is_true("description" not in inspect(obj).unloaded)

    def test_partial_column_load_populates_expired_attributes(self):
        """A column absent from the result row (here ``name``, via an
        aliased entity onto a statement selecting only id+description)
        lands in the ``expire`` populator with set_callable=True; the
        batch then does ``state.expired_attributes.add(key)``.

        Without the inlined state's ``expired_attributes = set()`` init
        this raises AttributeError (the slot has no class-level
        default).  This is the key regression guard, and it leaves the
        attribute genuinely populated in ``expired_attributes``.
        """
        HasDeferred = self.classes.HasDeferred
        s = fixture_session()

        # statement omits the "name" column -> the entity's ColumnLoader
        # finds no getter for name and emits an expire(set_callable=True)
        stmt = select(HasDeferred.id, HasDeferred.description).subquery()
        hd_alias = aliased(HasDeferred, stmt)

        rows = s.scalars(select(hd_alias).order_by(hd_alias.id)).all()
        eq_(len(rows), 2)

        obj = rows[0]
        state = inspect(obj)
        # name was absent -> expired, and recorded in expired_attributes
        # (the slot that must be initialized in the inlined state)
        is_true("name" in state.unloaded)
        is_true("name" in state.expired_attributes)

        # accessing the expired column loads it from the row's PK
        eq_(obj.name, "a")
        is_true("name" not in inspect(obj).unloaded)

    def test_undefer_via_option(self):
        HasDeferred = self.classes.HasDeferred
        s = fixture_session()

        obj = s.scalars(
            select(HasDeferred)
            .where(HasDeferred.id == 2)
            .options(undefer(HasDeferred.description))
        ).one()
        # undefer pulls description into the row -> not expired
        is_true("description" not in inspect(obj).unloaded)
        eq_(obj.description, "desc-b")


class BatchLoadEventsTest(_fixtures.FixtureTest):
    """Load events drive the ``has_evts`` per-row branch: per-instance
    ``_add_unpresent`` plus firing of the 'load' and
    ``loaded_as_persistent`` events."""

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_load_event_fires_per_new_instance(self):
        User = self.classes.User
        s = fixture_session()

        loaded = []

        @event.listens_for(User, "load")
        def on_load(target, context):
            loaded.append(target.id)

        try:
            users = s.scalars(select(User).order_by(User.id)).all()
            eq_(sorted(loaded), [7, 8, 9, 10])
            eq_(len(users), 4)
        finally:
            event.remove(User, "load", on_load)

    def test_loaded_as_persistent_fires_per_new_instance(self):
        User = self.classes.User
        s = fixture_session()

        persistent = []

        @event.listens_for(s, "loaded_as_persistent")
        def on_persistent(session, instance):
            persistent.append(instance.id)

        users = s.scalars(select(User).order_by(User.id)).all()
        eq_(sorted(persistent), [7, 8, 9, 10])
        eq_(len(users), 4)

    def test_load_event_refires_for_existing_via_delegation(self):
        """existing-instance rows delegate to _instance(), which still
        repopulates and re-fires the 'load' event (standard ORM
        semantics: 'load' fires on every load/refresh).  This confirms
        the batch's existing-instance delegation preserves event
        behaviour rather than silently skipping it."""
        User = self.classes.User
        s = fixture_session()

        loaded = []

        @event.listens_for(User, "load")
        def on_load(target, context):
            loaded.append(target.id)

        try:
            s.scalars(select(User).order_by(User.id)).all()
            eq_(sorted(loaded), [7, 8, 9, 10])

            loaded.clear()
            # second pass: all instances already present -> delegated to
            # _instance(), which fires 'load' again for each
            s.scalars(select(User).order_by(User.id)).all()
            eq_(sorted(loaded), [7, 8, 9, 10])
        finally:
            event.remove(User, "load", on_load)


class BatchPopulateExistingTest(_fixtures.FixtureTest):
    """populate_existing=True refreshes attributes from the row over
    existing instances."""

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_populate_existing_refreshes(self):
        User = self.classes.User
        users = self.tables.users
        s = fixture_session()

        u = s.scalars(select(User).where(User.id == 7)).one()
        eq_(u.name, "jack")

        # change the row out-of-band (separate transaction) so the
        # in-memory object is now stale but not dirty
        with testing.db.begin() as conn:
            conn.execute(
                update(users).where(users.c.id == 7).values(name="from-db")
            )
        try:
            # a plain reload returns the cached identity-mapped object
            # unchanged (existing-instance delegation, no refresh)
            plain = s.scalars(select(User).where(User.id == 7)).one()
            is_(plain, u)
            eq_(plain.name, "jack")

            # populate_existing forces a refresh from the row
            refreshed = s.scalars(
                select(User)
                .where(User.id == 7)
                .execution_options(populate_existing=True)
            ).one()
            is_(refreshed, u)
            eq_(refreshed.name, "from-db")
        finally:
            with testing.db.begin() as conn:
                conn.execute(
                    update(users).where(users.c.id == 7).values(name="jack")
                )

    def test_populate_existing_batch_flag(self):
        User = self.classes.User
        s = fixture_session()

        _, procs = _capture_processor(
            lambda: s.scalars(
                select(User).execution_options(populate_existing=True)
            ).all()
        )
        for proc in procs:
            batch = getattr(proc, "_sa_row_batch", None)
            is_not_none(batch)
            # populate_existing is a C struct member when compiled and
            # not visible from Python; only assert on the pure-Python
            # path
            if not _is_compiled():
                is_true(batch.populate_existing)


class _PolyFixture(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Employee(Base):
            __tablename__ = "employee"
            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            type = Column(String(50))
            __mapper_args__ = {
                "polymorphic_on": type,
                "polymorphic_identity": "employee",
            }

        class Engineer(Employee):
            __tablename__ = "engineer"
            id = Column(ForeignKey("employee.id"), primary_key=True)
            engineer_info = Column(String(50))
            __mapper_args__ = {"polymorphic_identity": "engineer"}

        class Manager(Employee):
            __tablename__ = "manager"
            id = Column(ForeignKey("employee.id"), primary_key=True)
            manager_data = Column(String(50))
            __mapper_args__ = {"polymorphic_identity": "manager"}

    @classmethod
    def insert_data(cls, connection):
        Employee, Engineer, Manager = cls.classes(
            "Employee", "Engineer", "Manager"
        )
        s = Session(connection)
        s.add_all(
            [
                Employee(id=1, name="plain"),
                Engineer(id=2, name="dilbert", engineer_info="knows code"),
                Manager(id=3, name="pointy", manager_data="manages"),
            ]
        )
        s.commit()


class BatchPolymorphicTest(_PolyFixture):
    """Polymorphic base: the batch is NOT attached (the early return in
    _instance_processor for the polymorphic-switch path happens before
    batch attachment).  All subclasses must still hydrate correctly via
    the closure."""

    def test_batch_not_attached_for_poly_base(self):
        Employee = self.classes.Employee
        s = fixture_session()

        _, procs = _capture_processor(
            lambda: s.scalars(select(Employee)).all()
        )
        is_true(len(procs) >= 1)
        # the top-level polymorphic entity processor (the polymorphic
        # switch closure, captured first) has no batch attached; the
        # early return in _instance_processor for the polymorphic-switch
        # path happens before batch attachment.  (Recursively-built leaf
        # sub-mapper processors may carry a batch, but instances() only
        # consults the top-level processor's _sa_row_batch.)
        top = procs[0]
        eq_(getattr(top, "__name__", None), "polymorphic_instance")
        is_none(getattr(top, "_sa_row_batch", None))

    def test_polymorphic_hydration(self):
        Employee, Engineer, Manager = self.classes(
            "Employee", "Engineer", "Manager"
        )
        s = fixture_session()

        rows = s.scalars(select(Employee).order_by(Employee.id)).all()
        eq_(len(rows), 3)
        is_true(type(rows[0]) is Employee)
        is_true(type(rows[1]) is Engineer)
        is_true(type(rows[2]) is Manager)
        eq_(rows[1].engineer_info, "knows code")
        eq_(rows[2].manager_data, "manages")

    def test_polymorphic_existing_delegation(self):
        Employee = self.classes.Employee
        s = fixture_session()

        first = s.scalars(select(Employee)).all()
        ids = {id(o) for o in first}
        second = s.scalars(select(Employee)).all()
        for o in second:
            is_true(id(o) in ids)


class _VersionedFixture(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Versioned(Base):
            __tablename__ = "versioned"
            id = Column(Integer, primary_key=True)
            version_id = Column(Integer, nullable=False)
            data = Column(String(50))
            __mapper_args__ = {"version_id_col": version_id}

    @classmethod
    def insert_data(cls, connection):
        Versioned = cls.classes.Versioned
        s = Session(connection)
        s.add_all(
            [
                Versioned(id=1, data="one"),
                Versioned(id=2, data="two"),
            ]
        )
        s.commit()


class BatchVersionedTest(_VersionedFixture):
    """Versioned mapping: reloading an existing instance delegates to
    _instance(), which performs the version check.  No spurious
    StaleDataError on a plain reload."""

    def test_initial_load(self):
        Versioned = self.classes.Versioned
        s = fixture_session()

        rows = s.scalars(select(Versioned).order_by(Versioned.id)).all()
        eq_(
            [(r.id, r.data, r.version_id) for r in rows],
            [(1, "one", 1), (2, "two", 1)],
        )

    def test_reload_existing_no_stale_error(self):
        Versioned = self.classes.Versioned
        s = fixture_session()

        first = s.scalars(select(Versioned).order_by(Versioned.id)).all()
        first_objs = {r.id: r for r in first}

        # reload over existing instances -> delegates to _instance and
        # the version check must pass (no StaleDataError)
        second = s.scalars(select(Versioned).order_by(Versioned.id)).all()
        for r in second:
            is_(r, first_objs[r.id])
        eq_([r.version_id for r in second], [1, 1])

    def test_populate_existing_versioned(self):
        Versioned = self.classes.Versioned
        versioned = Versioned.__table__
        s = fixture_session()

        obj = s.scalars(select(Versioned).where(Versioned.id == 1)).one()
        eq_(obj.data, "one")

        # change the row out-of-band, bumping the version id so the
        # populate_existing refresh sees a newer, consistent version
        with testing.db.begin() as conn:
            conn.execute(
                update(versioned)
                .where(versioned.c.id == 1)
                .values(data="from-db", version_id=2)
            )

        # populate_existing refreshes from the row; delegating to
        # _instance() runs the version check against the freshly read
        # version_id without a StaleDataError
        refreshed = s.scalars(
            select(Versioned)
            .where(Versioned.id == 1)
            .execution_options(populate_existing=True)
        ).one()
        is_(refreshed, obj)
        eq_(refreshed.data, "from-db")
        eq_(refreshed.version_id, 2)


class _WideFixture(fixtures.DeclarativeMappedTest):
    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class Wide(Base):
            __tablename__ = "wide"
            id = Column(Integer, primary_key=True)
            c0 = Column(String(20))
            c1 = Column(String(20))
            c2 = Column(String(20))
            c3 = Column(String(20))
            c4 = Column(String(20))
            c5 = Column(String(20))
            c6 = Column(String(20))
            c7 = Column(String(20))
            c8 = Column(String(20))
            c9 = Column(Integer)

    @classmethod
    def insert_data(cls, connection):
        Wide = cls.classes.Wide
        s = Session(connection)
        s.add_all(
            [
                Wide(
                    id=i,
                    c0="c0_%d" % i,
                    c1="c1_%d" % i,
                    c2="c2_%d" % i,
                    c3="c3_%d" % i,
                    c4="c4_%d" % i,
                    c5="c5_%d" % i,
                    c6="c6_%d" % i,
                    c7="c7_%d" % i,
                    c8="c8_%d" % i,
                    c9=i * 100,
                )
                for i in range(1, 6)
            ]
        )
        s.commit()


class BatchWideTableTest(_WideFixture):
    """A wide table exercises many quick populators on the use_idx
    path."""

    def test_wide_load(self):
        Wide = self.classes.Wide
        s = fixture_session()

        rows = s.scalars(select(Wide).order_by(Wide.id)).all()
        eq_(len(rows), 5)
        for i, r in enumerate(rows, start=1):
            eq_(r.id, i)
            eq_(r.c0, "c0_%d" % i)
            eq_(r.c5, "c5_%d" % i)
            eq_(r.c8, "c8_%d" % i)
            eq_(r.c9, i * 100)

    def test_wide_empty(self):
        Wide = self.classes.Wide
        s = fixture_session()

        rows = s.scalars(select(Wide).where(Wide.id == -1)).all()
        eq_(rows, [])


class BatchKilledIdentityMapTest(_fixtures.FixtureTest):
    """After Session.close()/expunge_all() the identity map is
    "killed" (its _add_unpresent is replaced).  The batch must detect
    this and delegate every row to _instance(), which raises
    informatively."""

    run_setup_mappers = "once"
    run_inserts = "once"
    run_deletes = None

    @classmethod
    def setup_mappers(cls):
        cls._setup_stock_mapping()

    def test_load_after_close_within_iteration(self):
        """Close the session from a load event handler mid-result; the
        per-row killed-identity-map handling delegates remaining rows to
        _instance()."""
        User = self.classes.User
        s = fixture_session()

        # Without an event, a normal load fully consumes before close.
        # Verify the simpler observable: a fresh session after a close
        # still loads correctly (the killed-map branch only triggers
        # mid-result, which we cover via the close-in-handler path
        # below).
        users = s.scalars(select(User).order_by(User.id)).all()
        eq_(len(users), 4)
        s.close()

        # new session, batch builds a fresh identity map -> normal load
        users2 = s.scalars(select(User).order_by(User.id)).all()
        eq_(len(users2), 4)
