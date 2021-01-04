from sqlalchemy import and_
from sqlalchemy import exc
from sqlalchemy import or_
from sqlalchemy import testing
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

    def test_col_expression_base_plus_two_subs(self):
        sess = fixture_session()
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
