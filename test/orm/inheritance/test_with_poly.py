from sqlalchemy import and_
from sqlalchemy import exc
from sqlalchemy import inspect
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy.orm import aliased
from sqlalchemy.orm import joinedload
from sqlalchemy.orm import with_polymorphic
from sqlalchemy.testing import eq_
from sqlalchemy.testing.fixtures import fixture_session
from ._poly_fixtures import _Polymorphic
from ._poly_fixtures import _PolymorphicAliasedJoins
from ._poly_fixtures import _PolymorphicFixtureBase
from ._poly_fixtures import _PolymorphicJoins
from ._poly_fixtures import _PolymorphicPolymorphic
from ._poly_fixtures import _PolymorphicUnions
from ._poly_fixtures import Boss
from ._poly_fixtures import Engineer
from ._poly_fixtures import Manager
from ._poly_fixtures import Person


class WithPolymorphicAPITest(_Polymorphic, _PolymorphicFixtureBase):
    __dialect__ = "default"

    def test_no_use_flat_and_aliased(self):
        sess = fixture_session()

        subq = sess.query(Person).subquery()

        testing.assert_raises_message(
            exc.ArgumentError,
            "the 'flat' and 'selectable' arguments cannot be passed "
            "simultaneously to with_polymorphic()",
            with_polymorphic,
            Person,
            [Engineer],
            selectable=subq,
            flat=True,
        )


# the polymorphic selectable rendered by
# with_polymorphic(Person, [Engineer], aliased=True)
_POLY_SUBQUERY = (
    "SELECT people.person_id AS people_person_id, "
    "people.company_id AS people_company_id, "
    "people.name AS people_name, people.type AS people_type, "
    "engineers.person_id AS engineers_person_id, "
    "engineers.status AS engineers_status, "
    "engineers.engineer_name AS engineers_engineer_name, "
    "engineers.primary_language AS engineers_primary_language "
    "FROM people LEFT OUTER JOIN engineers "
    "ON people.person_id = engineers.person_id"
)


# columns of with_polymorphic(Person, [Engineer]) as labeled within a
# subquery, following the given alias name
_POLY_SUBQUERY_COLS = (
    "{a}.people_person_id, {a}.people_company_id, {a}.people_name, "
    "{a}.people_type, {a}.engineers_person_id, {a}.engineers_status, "
    "{a}.engineers_engineer_name, {a}.engineers_primary_language"
)


class AliasOfPolymorphicTest(_Polymorphic, _PolymorphicFixtureBase):
    """aliased() of a with_polymorphic() entity, with no explicit
    selectable passed to aliased().

    The polymorphic selectable and the additional mappers are retained,
    #13584.

    """

    __dialect__ = "default"

    @testing.variation("wp_aliased", [True, False])
    def test_wpoly_alone(self, wp_aliased):
        """contrast case: the with_polymorphic() entity used directly"""
        pa = with_polymorphic(Person, [Engineer], aliased=bool(wp_aliased))

        if wp_aliased:
            self.assert_compile(
                select(pa),
                f"SELECT {_POLY_SUBQUERY_COLS.format(a='anon_1')} "
                f"FROM ({_POLY_SUBQUERY}) AS anon_1",
            )
        else:
            self.assert_compile(
                select(pa),
                "SELECT people.person_id, people.company_id, people.name, "
                "people.type, engineers.person_id AS person_id_1, "
                "engineers.status, engineers.engineer_name, "
                "engineers.primary_language "
                "FROM people LEFT OUTER JOIN engineers "
                "ON people.person_id = engineers.person_id",
            )

    @testing.variation("wp_aliased", [True, False])
    @testing.variation("alias_kind", ["plain", "named", "flat", "named_flat"])
    def test_aliased_of_wpoly(self, wp_aliased, alias_kind):
        pa = with_polymorphic(Person, [Engineer], aliased=bool(wp_aliased))

        aliased_kw = {
            "plain": {},
            "named": {"name": "pa2"},
            "flat": {"flat": True},
            "named_flat": {"name": "pa2", "flat": True},
        }[alias_kind.name]

        stmt = select(aliased(pa, **aliased_kw))

        if alias_kind.named or (wp_aliased and alias_kind.named_flat):
            # the polymorphic selectable is aliased as a subquery with
            # the given name; when it is already a subquery, "flat" has
            # no effect
            self.assert_compile(
                stmt,
                f"SELECT {_POLY_SUBQUERY_COLS.format(a='pa2')} "
                f"FROM ({_POLY_SUBQUERY}) AS pa2",
            )
        elif alias_kind.named_flat:
            self.assert_compile(
                stmt,
                "SELECT pa2_people.person_id, pa2_people.company_id, "
                "pa2_people.name, pa2_people.type, "
                "pa2_engineers.person_id AS person_id_1, "
                "pa2_engineers.status, pa2_engineers.engineer_name, "
                "pa2_engineers.primary_language "
                "FROM people AS pa2_people LEFT OUTER JOIN engineers "
                "AS pa2_engineers ON pa2_people.person_id = "
                "pa2_engineers.person_id",
            )
        elif alias_kind.flat and not wp_aliased:
            self.assert_compile(
                stmt,
                "SELECT people_1.person_id, people_1.company_id, "
                "people_1.name, people_1.type, "
                "engineers_1.person_id AS person_id_1, "
                "engineers_1.status, engineers_1.engineer_name, "
                "engineers_1.primary_language "
                "FROM people AS people_1 LEFT OUTER JOIN engineers "
                "AS engineers_1 ON people_1.person_id = "
                "engineers_1.person_id",
            )
        else:
            self.assert_compile(
                stmt,
                f"SELECT {_POLY_SUBQUERY_COLS.format(a='anon_1')} "
                f"FROM ({_POLY_SUBQUERY}) AS anon_1",
            )

    def test_aliased_of_aliased_of_wpoly(self):
        """aliased() of an aliased() of a with_polymorphic(); the
        polymorphic selectable is preserved through both calls and the
        given name is applied to it"""

        pa = with_polymorphic(Person, [Engineer])

        self.assert_compile(
            select(aliased(aliased(pa), name="a2")),
            f"SELECT {_POLY_SUBQUERY_COLS.format(a='a2')} "
            f"FROM ({_POLY_SUBQUERY}) AS a2",
        )

    def test_subclass_criteria(self):
        """criteria against a subclass attribute of the aliased entity
        refers to the aliased polymorphic selectable"""

        pa = aliased(
            with_polymorphic(Person, [Engineer]), name="pa2", flat=True
        )

        self.assert_compile(
            select(pa.name).where(pa.Engineer.primary_language == "java"),
            "SELECT pa2_people.name FROM people AS pa2_people "
            "LEFT OUTER JOIN engineers AS pa2_engineers "
            "ON pa2_people.person_id = pa2_engineers.person_id "
            "WHERE pa2_engineers.primary_language = :primary_language_1",
        )


class _WithPolymorphicBase(_PolymorphicFixtureBase):
    def test_join_base_to_sub(self):
        sess = fixture_session()
        pa = with_polymorphic(Person, [Engineer])

        def go():
            eq_(
                sess.query(pa)
                .filter(pa.Engineer.primary_language == "java")
                .all(),
                self._emps_wo_relationships_fixture()[0:1],
            )

        self.assert_sql_count(testing.db, go, 1)

    @testing.combinations((True,), (False,), argnames="use_star")
    def test_col_expression_base_plus_two_subs(self, use_star):
        sess = fixture_session()

        if use_star:
            pa = with_polymorphic(Person, "*")
        else:
            pa = with_polymorphic(Person, [Engineer, Manager])

        eq_(
            sess.query(
                pa.name, pa.Engineer.primary_language, pa.Manager.manager_name
            )
            .filter(
                or_(
                    pa.Engineer.primary_language == "java",
                    pa.Manager.manager_name == "dogbert",
                )
            )
            .order_by(pa.Engineer.type)
            .all(),
            [("dilbert", "java", None), ("dogbert", None, "dogbert")],
        )

    def test_orm_entity_w_gc(self):
        """test #6680"""
        sess = fixture_session()

        stmt = select(with_polymorphic(Person, "*"))

        eq_(len(sess.execute(stmt).all()), 5)

    @testing.combinations(
        ({},),
        ({"aliased": True},),
        ({"flat": True},),
        ({"innerjoin": True},),
        argnames="wp_kw",
    )
    @testing.variation("alias_kind", ["none", "plain", "named", "flat"])
    def test_aliased_of_with_polymorphic_rows(self, wp_kw, alias_kind):
        """aliased() of a with_polymorphic() entity with no explicit
        selectable returns the same rows as the with_polymorphic() entity,
        including criteria against subclass attributes, #13584

        """
        sess = fixture_session()

        wp = with_polymorphic(Person, [Engineer], **wp_kw)
        if alias_kind.none:
            entity = wp
        else:
            entity = aliased(
                wp,
                **{
                    "plain": {},
                    "named": {"name": "pa2"},
                    "flat": {"flat": True},
                }[alias_kind.name],
            )

        eq_(
            inspect(entity).with_polymorphic_mappers,
            inspect(wp).with_polymorphic_mappers,
        )

        if wp_kw.get("innerjoin") and self.select_type in (
            "",
            "Polymorphic",
        ):
            # innerjoin takes effect only when the mapper does not
            # configure its own polymorphic selectable
            expected_rows = [
                ("dilbert", "java"),
                ("wally", "c++"),
                ("vlad", "cobol"),
            ]
        else:
            expected_rows = [
                ("dilbert", "java"),
                ("wally", "c++"),
                ("pointy haired boss", None),
                ("dogbert", None),
                ("vlad", "cobol"),
            ]

        eq_(
            sess.execute(
                select(entity.name, entity.Engineer.primary_language).order_by(
                    entity.person_id
                )
            ).all(),
            expected_rows,
        )

        eq_(
            sess.scalars(
                select(entity.name).where(
                    entity.Engineer.primary_language == "java"
                )
            ).all(),
            ["dilbert"],
        )

    @testing.variation("use_aliased", [True, False])
    def test_aliased_of_with_polymorphic_joinedload_innerjoin(
        self, use_aliased
    ):
        """aliased() of an outer-joined with_polymorphic() entity still
        represents an outer join, so that an innerjoin joined eager load
        from a subclass relationship does not exclude rows of other
        subclasses, #13584

        """
        sess = fixture_session()

        wp = with_polymorphic(Person, [Engineer])
        entity = aliased(wp) if use_aliased else wp

        stmt = (
            select(entity)
            .options(joinedload(entity.Engineer.machines, innerjoin=True))
            .order_by(entity.person_id)
        )
        with self.assert_statement_count(testing.db, 1):
            eq_(
                [
                    (
                        p.name,
                        (
                            [m.name for m in p.machines]
                            if isinstance(p, Engineer)
                            else None
                        ),
                    )
                    for p in sess.scalars(stmt).unique()
                ],
                [
                    ("dilbert", ["IBM ThinkPad", "IPhone"]),
                    ("wally", ["Commodore 64"]),
                    ("pointy haired boss", None),
                    ("dogbert", None),
                    ("vlad", ["Commodore 64", "IBM 3270"]),
                ],
            )

    @testing.combinations("subquery", "cte", argnames="kind")
    def test_aliased_of_aliased_selectable(self, kind):
        """aliased() of an entity that is aliased() against a subquery or
        CTE preserves that selectable, #13583

        """
        sess = fixture_session()

        stmt = select(Person).where(Person.name.in_(["dilbert", "wally"]))
        if kind == "subquery":
            selectable = stmt.subquery()
        else:
            selectable = stmt.cte()

        p1 = aliased(Person, selectable)
        p2 = aliased(p1)
        p3 = aliased(p2)

        for entity in (p1, p2, p3):
            eq_(
                sess.scalars(
                    select(entity.name).order_by(entity.person_id)
                ).all(),
                ["dilbert", "wally"],
            )

    def test_join_to_join_entities(self):
        sess = fixture_session()
        pa = with_polymorphic(Person, [Engineer])
        pa_alias = with_polymorphic(Person, [Engineer], aliased=True)

        eq_(
            [
                (p1.name, type(p1), p2.name, type(p2))
                for (p1, p2) in sess.query(pa, pa_alias)
                .join(
                    pa_alias,
                    or_(
                        pa.Engineer.primary_language
                        == pa_alias.Engineer.primary_language,
                        and_(
                            pa.Engineer.primary_language == None,  # noqa
                            pa_alias.Engineer.primary_language == None,
                            pa.person_id > pa_alias.person_id,
                        ),
                    ),
                )
                .order_by(pa.name, pa_alias.name)
            ],
            [
                ("dilbert", Engineer, "dilbert", Engineer),
                ("dogbert", Manager, "pointy haired boss", Boss),
                ("vlad", Engineer, "vlad", Engineer),
                ("wally", Engineer, "wally", Engineer),
            ],
        )

    def test_join_to_join_columns(self):
        sess = fixture_session()
        pa = with_polymorphic(Person, [Engineer])
        pa_alias = with_polymorphic(Person, [Engineer], aliased=True)

        eq_(
            [
                row
                for row in sess.query(
                    pa.name,
                    pa.Engineer.primary_language,
                    pa_alias.name,
                    pa_alias.Engineer.primary_language,
                )
                .join(
                    pa_alias,
                    or_(
                        pa.Engineer.primary_language
                        == pa_alias.Engineer.primary_language,
                        and_(
                            pa.Engineer.primary_language == None,  # noqa
                            pa_alias.Engineer.primary_language == None,
                            pa.person_id > pa_alias.person_id,
                        ),
                    ),
                )
                .order_by(pa.name, pa_alias.name)
            ],
            [
                ("dilbert", "java", "dilbert", "java"),
                ("dogbert", None, "pointy haired boss", None),
                ("vlad", "cobol", "vlad", "cobol"),
                ("wally", "c++", "wally", "c++"),
            ],
        )


class PolymorphicTest(_WithPolymorphicBase, _Polymorphic):
    pass


class PolymorphicPolymorphicTest(
    _WithPolymorphicBase, _PolymorphicPolymorphic
):
    pass


class PolymorphicUnionsTest(_WithPolymorphicBase, _PolymorphicUnions):
    pass


class PolymorphicAliasedJoinsTest(
    _WithPolymorphicBase, _PolymorphicAliasedJoins
):
    pass


class PolymorphicJoinsTest(_WithPolymorphicBase, _PolymorphicJoins):
    pass
