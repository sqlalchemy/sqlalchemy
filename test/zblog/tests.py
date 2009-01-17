import testenv; testenv.configure_for_tests()
from sqlalchemy import *
from sqlalchemy.orm import *
from testlib import *
from zblog import mappers, tables
from zblog.user import *
from zblog.blog import *


class ZBlogTest(TestBase, AssertsExecutionResults):

    def create_tables(self):
        tables.metadata.drop_all(bind=testing.db)
        tables.metadata.create_all(bind=testing.db)
    def drop_tables(self):
        tables.metadata.drop_all(bind=testing.db)

    def setUpAll(self):
        self.create_tables()
    def tearDownAll(self):
        self.drop_tables()
    def tearDown(self):
        pass
    def setUp(self):
        pass


class SavePostTest(ZBlogTest):
    def setUpAll(self):
        super(SavePostTest, self).setUpAll()
        mappers.zblog_mappers()
        global blog_id, user_id
        s = create_session(bind=testing.db)
        user = User('zbloguser', "Zblog User", "hello", group=administrator)
        blog = Blog(owner=user)
        blog.name = "this is a blog"
        s.add(user)
        s.add(blog)
        s.flush()
        blog_id = blog.id
        user_id = user.id
        s.close()

    def tearDownAll(self):
        clear_mappers()
        super(SavePostTest, self).tearDownAll()

    def testattach(self):
        """test that a transient/pending instance has proper bi-directional behavior.

        this requires that lazy loaders do not fire off for a transient/pending instance."""
        s = create_session(bind=testing.db)

        s.begin()
        try:
            blog = s.query(Blog).get(blog_id)
            post = Post(headline="asdf asdf", summary="asdfasfd")
            s.add(post)
            post.blog_id=blog_id
            post.blog = blog
            assert post in blog.posts
        finally:
            s.rollback()

    def testoptimisticorphans(self):
        """test that instances in the session with un-loaded parents will not
        get marked as "orphans" and then deleted """
        s = create_session(bind=testing.db)

        s.begin()
        try:
            blog = s.query(Blog).get(blog_id)
            post = Post(headline="asdf asdf", summary="asdfasfd")
            post.blog = blog
            user = s.query(User).get(user_id)
            post.user = user
            s.add(post)
            s.flush()
            s.expunge_all()

            user = s.query(User).get(user_id)
            blog = s.query(Blog).get(blog_id)
            post = blog.posts[0]
            comment = Comment(subject="some subject", body="some body")
            comment.post = post
            comment.user = user
            s.flush()
            s.expunge_all()

            assert s.query(Post).get(post.id) is not None

        finally:
            s.rollback()


if __name__ == "__main__":
    testenv.main()
