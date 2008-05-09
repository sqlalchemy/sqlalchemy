from testlib import testing


class AltEngineTest(testing.TestBase):
    engine = None

    def setUpAll(self):
        type(self).engine = self.create_engine()
        testing.TestBase.setUpAll(self)

    def tearDownAll(self):
        testing.TestBase.tearDownAll(self)
        self.engine.dispose()
        type(self).engine = None

    def create_engine(self):
        raise NotImplementedError
