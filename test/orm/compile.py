from sqlalchemy import *
import testbase

class CompileTest(testbase.AssertMixin):
    """test various mapper compilation scenarios"""
    def tearDownAll(self):
        clear_mappers()
        
    def testone(self):
        global metadata, order, employee, product, tax, orderproduct
        metadata = BoundMetaData(engine)

        order = Table('orders', metadata, 
            Column('id', Integer, primary_key=True),
            Column('employee_id', Integer, ForeignKey('employees.id'), nullable=False),
            Column('type', Unicode(16)))

        employee = Table('employees', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', Unicode(16), unique=True, nullable=False))

        product = Table('products', metadata,
            Column('id', Integer, primary_key=True),
        )

        orderproduct = Table('orderproducts', metadata,
            Column('id', Integer, primary_key=True),
            Column('order_id', Integer, ForeignKey("orders.id"), nullable=False),
            Column('product_id', Integer, ForeignKey("products.id"), nullable=False),
        )

        class Order(object):
            pass

        class Employee(object):
            pass

        class Product(object):
            pass

        class OrderProduct(object):
            pass

        order_join = order.select().alias('pjoin')

        order_mapper = mapper(Order, order, 
            select_table=order_join, 
            polymorphic_on=order_join.c.type, 
            polymorphic_identity='order',
            properties={
                'orderproducts': relation(OrderProduct, lazy=True, backref='order')}
            )

        mapper(Product, product,
            properties={
                'orderproducts': relation(OrderProduct, lazy=True, backref='product')}
            )

        mapper(Employee, employee,
            properties={
                'orders': relation(Order, lazy=True, backref='employee')})

        mapper(OrderProduct, orderproduct)
        
        # this requires that the compilation of order_mapper's "surrogate mapper" occur after
        # the initial setup of MapperProperty objects on the mapper.
        class_mapper(Product).compile()

    def testtwo(self):
        """test that conflicting backrefs raises an exception"""
        global metadata, order, employee, product, tax, orderproduct
        metadata = BoundMetaData(engine)

        order = Table('orders', metadata, 
            Column('id', Integer, primary_key=True),
            Column('type', Unicode(16)))

        product = Table('products', metadata,
            Column('id', Integer, primary_key=True),
        )

        orderproduct = Table('orderproducts', metadata,
            Column('id', Integer, primary_key=True),
            Column('order_id', Integer, ForeignKey("orders.id"), nullable=False),
            Column('product_id', Integer, ForeignKey("products.id"), nullable=False),
        )

        class Order(object):
            pass

        class Product(object):
            pass

        class OrderProduct(object):
            pass

        order_join = order.select().alias('pjoin')

        order_mapper = mapper(Order, order, 
            select_table=order_join, 
            polymorphic_on=order_join.c.type, 
            polymorphic_identity='order',
            properties={
                'orderproducts': relation(OrderProduct, lazy=True, backref='product')}
            )

        mapper(Product, product,
            properties={
                'orderproducts': relation(OrderProduct, lazy=True, backref='product')}
            )

        mapper(OrderProduct, orderproduct)

        try:
            class_mapper(Product).compile()
            assert False
        except exceptions.ArgumentError, e:
            assert str(e).index("Backrefs do not match") > -1

if __name__ == '__main__':
    testbase.main()
