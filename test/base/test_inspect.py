"""test the inspection registry system."""

from sqlalchemy.testing import eq_, assert_raises_message
from sqlalchemy import exc, util
from sqlalchemy import inspection, inspect
from sqlalchemy.testing import fixtures


class TestFixture(object):
    pass


class TestInspection(fixtures.TestBase):

    def tearDown(self):
        for type_ in list(inspection._registrars):
            if issubclass(type_, TestFixture):
                del inspection._registrars[type_]

    def test_def_insp(self):
        class SomeFoo(TestFixture):
            pass

        @inspection._inspects(SomeFoo)
        def insp_somefoo(subject):
            return {"insp": subject}

        somefoo = SomeFoo()
        insp = inspect(somefoo)
        assert insp["insp"] is somefoo

    def test_no_inspect(self):
        class SomeFoo(TestFixture):
            pass

        assert_raises_message(
            exc.NoInspectionAvailable,
            "No inspection system is available for object of type ",
            inspect, SomeFoo
        )

    def test_class_insp(self):
        class SomeFoo(TestFixture):
            pass

        class SomeFooInspect(object):
            def __init__(self, target):
                self.target = target
        SomeFooInspect = inspection._inspects(SomeFoo)(SomeFooInspect)

        somefoo = SomeFoo()
        insp = inspect(somefoo)
        assert isinstance(insp, SomeFooInspect)
        assert insp.target is somefoo

    def test_hierarchy_insp(self):
        class SomeFoo(TestFixture):
            pass

        class SomeSubFoo(SomeFoo):
            pass

        @inspection._inspects(SomeFoo)
        def insp_somefoo(subject):
            return 1

        @inspection._inspects(SomeSubFoo)
        def insp_somesubfoo(subject):
            return 2

        somefoo = SomeFoo()
        eq_(inspect(SomeFoo()), 1)
        eq_(inspect(SomeSubFoo()), 2)
