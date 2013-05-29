from sqlalchemy import Integer, String, ForeignKey, func, desc, and_, or_
from sqlalchemy.orm import interfaces, relationship, mapper, \
    clear_mappers, create_session, joinedload, joinedload_all, \
    subqueryload, subqueryload_all, polymorphic_union, aliased,\
    class_mapper, with_polymorphic
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import default

from sqlalchemy.testing import AssertsCompiledSQL, fixtures
from sqlalchemy import testing
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.testing import assert_raises, eq_

from ._poly_fixtures import Company, Person, Engineer, Manager, Boss, \
    Machine, Paperwork, _PolymorphicFixtureBase, _Polymorphic,\
    _PolymorphicPolymorphic, _PolymorphicUnions, _PolymorphicJoins,\
    _PolymorphicAliasedJoins

class _WithPolymorphicBase(_PolymorphicFixtureBase):
    def test_join_base_to_sub(self):
        sess = create_session()
        pa = with_polymorphic(Person, [Engineer])

        def go():
            eq_(sess.query(pa)
                    .filter(pa.Engineer.primary_language == 'java').all(),
                self._emps_wo_relationships_fixture()[0:1])
        self.assert_sql_count(testing.db, go, 1)

    def test_col_expression_base_plus_two_subs(self):
        sess = create_session()
        pa = with_polymorphic(Person, [Engineer, Manager])

        eq_(
            sess.query(pa.name, pa.Engineer.primary_language, pa.Manager.manager_name).\
                filter(or_(pa.Engineer.primary_language=='java',
                                pa.Manager.manager_name=='dogbert')).\
                order_by(pa.Engineer.type).all(),
            [
                ('dilbert', 'java', None),
                ('dogbert', None, 'dogbert'),
            ]
        )


    def test_join_to_join_entities(self):
        sess = create_session()
        pa = with_polymorphic(Person, [Engineer])
        pa_alias = with_polymorphic(Person, [Engineer], aliased=True)

        eq_(
            [(p1.name, type(p1), p2.name, type(p2)) for (p1, p2) in sess.query(
                pa, pa_alias
            ).join(pa_alias,
                    or_(
                        pa.Engineer.primary_language==\
                        pa_alias.Engineer.primary_language,
                        and_(
                            pa.Engineer.primary_language == None,
                            pa_alias.Engineer.primary_language == None,
                            pa.person_id > pa_alias.person_id
                        )
                    )
                ).order_by(pa.name, pa_alias.name)],
            [
                ('dilbert', Engineer, 'dilbert', Engineer),
                ('dogbert', Manager, 'pointy haired boss', Boss),
                ('vlad', Engineer, 'vlad', Engineer),
                ('wally', Engineer, 'wally', Engineer)
            ]
        )

    def test_join_to_join_columns(self):
        sess = create_session()
        pa = with_polymorphic(Person, [Engineer])
        pa_alias = with_polymorphic(Person, [Engineer], aliased=True)

        eq_(
            [row for row in sess.query(
                pa.name, pa.Engineer.primary_language,
                pa_alias.name, pa_alias.Engineer.primary_language
            ).join(pa_alias,
                    or_(
                        pa.Engineer.primary_language==\
                        pa_alias.Engineer.primary_language,
                        and_(
                            pa.Engineer.primary_language == None,
                            pa_alias.Engineer.primary_language == None,
                            pa.person_id > pa_alias.person_id
                        )
                    )
                ).order_by(pa.name, pa_alias.name)],
            [
                ('dilbert', 'java', 'dilbert', 'java'),
                ('dogbert', None, 'pointy haired boss', None),
                ('vlad', 'cobol', 'vlad', 'cobol'),
                ('wally', 'c++', 'wally', 'c++')
            ]
        )

class PolymorphicTest(_WithPolymorphicBase, _Polymorphic):
    pass

class PolymorphicPolymorphicTest(_WithPolymorphicBase, _PolymorphicPolymorphic):
    pass

class PolymorphicUnionsTest(_WithPolymorphicBase, _PolymorphicUnions):
    pass

class PolymorphicAliasedJoinsTest(_WithPolymorphicBase, _PolymorphicAliasedJoins):
    pass

class PolymorphicJoinsTest(_WithPolymorphicBase, _PolymorphicJoins):
    pass
