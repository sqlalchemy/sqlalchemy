from sqlalchemy import Integer, ForeignKey, String, MetaData
from sqlalchemy.orm import relationship, mapper, create_session
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy import testing
from sqlalchemy.testing.schema import Table, Column
from sqlalchemy.testing.util import picklers


metadata = None


def step_numbering(step):
    """ order in whole steps """
    def f(index, collection):
        return step * index
    return f


def fibonacci_numbering(order_col):
    """
    almost fibonacci- skip the first 2 steps
    e.g. 1, 2, 3, 5, 8, ... instead of 0, 1, 1, 2, 3, ...
    otherwise ordering of the elements at '1' is undefined... ;)
    """
    def f(index, collection):
        if index == 0:
            return 1
        elif index == 1:
            return 2
        else:
            return (getattr(collection[index - 1], order_col) +
                    getattr(collection[index - 2], order_col))
    return f


def alpha_ordering(index, collection):
    """
    0 -> A, 1 -> B, ... 25 -> Z, 26 -> AA, 27 -> AB, ...
    """
    s = ''
    while index > 25:
        d = index / 26
        s += chr((d % 26) + 64)
        index -= d * 26
    s += chr(index + 65)
    return s


class OrderingListTest(fixtures.TestBase):
    def setup(self):
        global metadata, slides_table, bullets_table, Slide, Bullet
        slides_table, bullets_table = None, None
        Slide, Bullet = None, None
        metadata = MetaData(testing.db)

    def _setup(self, test_collection_class):
        """Build a relationship situation using the given
        test_collection_class factory"""

        global metadata, slides_table, bullets_table, Slide, Bullet

        slides_table = Table('test_Slides', metadata,
                             Column('id', Integer, primary_key=True,
                                    test_needs_autoincrement=True),
                             Column('name', String(128)))
        bullets_table = Table('test_Bullets', metadata,
                              Column('id', Integer, primary_key=True,
                                     test_needs_autoincrement=True),
                              Column('slide_id', Integer,
                                     ForeignKey('test_Slides.id')),
                              Column('position', Integer),
                              Column('text', String(128)))

        class Slide(object):
            def __init__(self, name):
                self.name = name

            def __repr__(self):
                return '<Slide "%s">' % self.name

        class Bullet(object):
            def __init__(self, text):
                self.text = text

            def __repr__(self):
                return '<Bullet "%s" pos %s>' % (self.text, self.position)

        mapper(Slide, slides_table, properties={
            'bullets': relationship(Bullet, lazy='joined',
                                    collection_class=test_collection_class,
                                    backref='slide',
                                    order_by=[bullets_table.c.position])})
        mapper(Bullet, bullets_table)

        metadata.create_all()

    def teardown(self):
        metadata.drop_all()

    def test_append_no_reorder(self):
        self._setup(ordering_list('position', count_from=1,
                                  reorder_on_append=False))

        s1 = Slide('Slide #1')

        self.assert_(not s1.bullets)
        self.assert_(len(s1.bullets) == 0)

        s1.bullets.append(Bullet('s1/b1'))

        self.assert_(s1.bullets)
        self.assert_(len(s1.bullets) == 1)
        self.assert_(s1.bullets[0].position == 1)

        s1.bullets.append(Bullet('s1/b2'))

        self.assert_(len(s1.bullets) == 2)
        self.assert_(s1.bullets[0].position == 1)
        self.assert_(s1.bullets[1].position == 2)

        bul = Bullet('s1/b100')
        bul.position = 100
        s1.bullets.append(bul)

        self.assert_(s1.bullets[0].position == 1)
        self.assert_(s1.bullets[1].position == 2)
        self.assert_(s1.bullets[2].position == 100)

        s1.bullets.append(Bullet('s1/b4'))
        self.assert_(s1.bullets[0].position == 1)
        self.assert_(s1.bullets[1].position == 2)
        self.assert_(s1.bullets[2].position == 100)
        self.assert_(s1.bullets[3].position == 4)

        s1.bullets._reorder()
        self.assert_(s1.bullets[0].position == 1)
        self.assert_(s1.bullets[1].position == 2)
        self.assert_(s1.bullets[2].position == 3)
        self.assert_(s1.bullets[3].position == 4)

        session = create_session()
        session.add(s1)
        session.flush()

        id = s1.id
        session.expunge_all()
        del s1

        srt = session.query(Slide).get(id)

        self.assert_(srt.bullets)
        self.assert_(len(srt.bullets) == 4)

        titles = ['s1/b1', 's1/b2', 's1/b100', 's1/b4']
        found = [b.text for b in srt.bullets]

        self.assert_(titles == found)

    def test_append_reorder(self):
        self._setup(ordering_list('position', count_from=1,
                                  reorder_on_append=True))

        s1 = Slide('Slide #1')

        self.assert_(not s1.bullets)
        self.assert_(len(s1.bullets) == 0)

        s1.bullets.append(Bullet('s1/b1'))

        self.assert_(s1.bullets)
        self.assert_(len(s1.bullets) == 1)
        self.assert_(s1.bullets[0].position == 1)

        s1.bullets.append(Bullet('s1/b2'))

        self.assert_(len(s1.bullets) == 2)
        self.assert_(s1.bullets[0].position == 1)
        self.assert_(s1.bullets[1].position == 2)

        bul = Bullet('s1/b100')
        bul.position = 100
        s1.bullets.append(bul)

        self.assert_(s1.bullets[0].position == 1)
        self.assert_(s1.bullets[1].position == 2)
        self.assert_(s1.bullets[2].position == 3)

        s1.bullets.append(Bullet('s1/b4'))
        self.assert_(s1.bullets[0].position == 1)
        self.assert_(s1.bullets[1].position == 2)
        self.assert_(s1.bullets[2].position == 3)
        self.assert_(s1.bullets[3].position == 4)

        s1.bullets._reorder()
        self.assert_(s1.bullets[0].position == 1)
        self.assert_(s1.bullets[1].position == 2)
        self.assert_(s1.bullets[2].position == 3)
        self.assert_(s1.bullets[3].position == 4)

        s1.bullets._raw_append(Bullet('raw'))
        self.assert_(s1.bullets[4].position is None)

        s1.bullets._reorder()
        self.assert_(s1.bullets[4].position == 5)
        session = create_session()
        session.add(s1)
        session.flush()

        id = s1.id
        session.expunge_all()
        del s1

        srt = session.query(Slide).get(id)

        self.assert_(srt.bullets)
        self.assert_(len(srt.bullets) == 5)

        titles = ['s1/b1', 's1/b2', 's1/b100', 's1/b4', 'raw']
        found = [b.text for b in srt.bullets]
        eq_(titles, found)

        srt.bullets._raw_append(Bullet('raw2'))
        srt.bullets[-1].position = 6
        session.flush()
        session.expunge_all()

        srt = session.query(Slide).get(id)
        titles = ['s1/b1', 's1/b2', 's1/b100', 's1/b4', 'raw', 'raw2']
        found = [b.text for b in srt.bullets]
        eq_(titles, found)

    def test_insert(self):
        self._setup(ordering_list('position'))

        s1 = Slide('Slide #1')
        s1.bullets.append(Bullet('1'))
        s1.bullets.append(Bullet('2'))
        s1.bullets.append(Bullet('3'))
        s1.bullets.append(Bullet('4'))

        self.assert_(s1.bullets[0].position == 0)
        self.assert_(s1.bullets[1].position == 1)
        self.assert_(s1.bullets[2].position == 2)
        self.assert_(s1.bullets[3].position == 3)

        s1.bullets.insert(2, Bullet('insert_at_2'))
        self.assert_(s1.bullets[0].position == 0)
        self.assert_(s1.bullets[1].position == 1)
        self.assert_(s1.bullets[2].position == 2)
        self.assert_(s1.bullets[3].position == 3)
        self.assert_(s1.bullets[4].position == 4)

        self.assert_(s1.bullets[1].text == '2')
        self.assert_(s1.bullets[2].text == 'insert_at_2')
        self.assert_(s1.bullets[3].text == '3')

        s1.bullets.insert(999, Bullet('999'))

        self.assert_(len(s1.bullets) == 6)
        self.assert_(s1.bullets[5].position == 5)

        session = create_session()
        session.add(s1)
        session.flush()

        id = s1.id
        session.expunge_all()
        del s1

        srt = session.query(Slide).get(id)

        self.assert_(srt.bullets)
        self.assert_(len(srt.bullets) == 6)

        texts = ['1', '2', 'insert_at_2', '3', '4', '999']
        found = [b.text for b in srt.bullets]

        self.assert_(texts == found)

    def test_slice(self):
        self._setup(ordering_list('position'))

        b = [Bullet('1'), Bullet('2'), Bullet('3'),
             Bullet('4'), Bullet('5'), Bullet('6')]
        s1 = Slide('Slide #1')

        # 1, 2, 3
        s1.bullets[0:3] = b[0:3]
        for i in 0, 1, 2:
            self.assert_(s1.bullets[i].position == i)
            self.assert_(s1.bullets[i] == b[i])

        # 1, 4, 5, 6, 3
        s1.bullets[1:2] = b[3:6]
        for li, bi in (0, 0), (1, 3), (2, 4), (3, 5), (4, 2):
            self.assert_(s1.bullets[li].position == li)
            self.assert_(s1.bullets[li] == b[bi])

        # 1, 6, 3
        del s1.bullets[1:3]
        for li, bi in (0, 0), (1, 5), (2, 2):
            self.assert_(s1.bullets[li].position == li)
            self.assert_(s1.bullets[li] == b[bi])

        session = create_session()
        session.add(s1)
        session.flush()

        id = s1.id
        session.expunge_all()
        del s1

        srt = session.query(Slide).get(id)

        self.assert_(srt.bullets)
        self.assert_(len(srt.bullets) == 3)

        texts = ['1', '6', '3']
        for i, text in enumerate(texts):
            self.assert_(srt.bullets[i].position == i)
            self.assert_(srt.bullets[i].text == text)

    def test_replace(self):
        self._setup(ordering_list('position'))

        s1 = Slide('Slide #1')
        s1.bullets = [Bullet('1'), Bullet('2'), Bullet('3')]

        self.assert_(len(s1.bullets) == 3)
        self.assert_(s1.bullets[2].position == 2)

        session = create_session()
        session.add(s1)
        session.flush()

        new_bullet = Bullet('new 2')
        self.assert_(new_bullet.position is None)

        # mark existing bullet as db-deleted before replacement.
        # session.delete(s1.bullets[1])
        s1.bullets[1] = new_bullet

        self.assert_(new_bullet.position == 1)
        self.assert_(len(s1.bullets) == 3)

        id = s1.id

        session.flush()
        session.expunge_all()

        srt = session.query(Slide).get(id)

        self.assert_(srt.bullets)
        self.assert_(len(srt.bullets) == 3)

        self.assert_(srt.bullets[1].text == 'new 2')
        self.assert_(srt.bullets[2].text == '3')

    def test_replace_two(self):
        """test #3191"""

        self._setup(ordering_list('position', reorder_on_append=True))

        s1 = Slide('Slide #1')

        b1, b2, b3, b4 = Bullet('1'), Bullet('2'), Bullet('3'), Bullet('4')
        s1.bullets = [b1, b2, b3]

        eq_(
            [b.position for b in s1.bullets],
            [0, 1, 2]
        )

        s1.bullets = [b4, b2, b1]
        eq_(
            [b.position for b in s1.bullets],
            [0, 1, 2]
        )

    def test_funky_ordering(self):
        class Pos(object):
            def __init__(self):
                self.position = None

        step_factory = ordering_list('position',
                                     ordering_func=step_numbering(2))

        stepped = step_factory()
        stepped.append(Pos())
        stepped.append(Pos())
        stepped.append(Pos())
        stepped.append(Pos())

        for li, pos in (0, 0), (1, 2), (2, 4), (3, 6):
            self.assert_(stepped[li].position == pos)

        fib_factory = ordering_list(
            'position',
            ordering_func=fibonacci_numbering('position'))

        fibbed = fib_factory()
        fibbed.append(Pos())
        fibbed.append(Pos())
        fibbed.append(Pos())
        fibbed.append(Pos())
        fibbed.append(Pos())

        for li, pos in (0, 1), (1, 2), (2, 3), (3, 5), (4, 8):
            self.assert_(fibbed[li].position == pos)

        fibbed.insert(2, Pos())
        fibbed.insert(4, Pos())
        fibbed.insert(6, Pos())

        for li, pos in (
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 5),
            (4, 8),
            (5, 13),
            (6, 21),
            (7, 34),
        ):
            self.assert_(fibbed[li].position == pos)

        alpha_factory = ordering_list('position',
                                      ordering_func=alpha_ordering)
        alpha = alpha_factory()
        alpha.append(Pos())
        alpha.append(Pos())
        alpha.append(Pos())

        alpha.insert(1, Pos())

        for li, pos in (0, 'A'), (1, 'B'), (2, 'C'), (3, 'D'):
            self.assert_(alpha[li].position == pos)

    def test_picklability(self):
        from sqlalchemy.ext.orderinglist import OrderingList

        olist = OrderingList('order', reorder_on_append=True)
        olist.append(DummyItem())

        for loads, dumps in picklers():
            pck = dumps(olist)
            copy = loads(pck)

            self.assert_(copy == olist)
            self.assert_(copy.__dict__ == olist.__dict__)


class DummyItem(object):
    def __init__(self, order=None):
        self.order = order

    def __eq__(self, other):
        return self.order == other.order

    def __ne__(self, other):
        return not (self == other)
