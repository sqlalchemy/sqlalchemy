<%flags>inherit='document_base.myt'</%flags>
<%attr>title='The Types System'</%attr>

<&|doclib.myt:item, name="types", description="The Types System" &>
<p>The package <span class="codeline">sqlalchemy.types</span> defines the datatype identifiers which may be used when defining <&formatting.myt:link, path="metadata", text="table metadata"&>.  This package includes a set of generic types, a set of SQL-specific subclasses of those types, and a small extension system used by specific database connectors to adapt these generic types into database-specific type objects.
</p>
<&|doclib.myt:item, name="standard", description="Built-in Types" &>

<p>SQLAlchemy comes with a set of standard generic datatypes, which are defined as classes.   
</p>
<p>The standard set of generic types are:</p>
<&|formatting.myt:code, title="package sqlalchemy.types"&>
	class String(TypeEngine):
	    def __init__(self, length=None)
	    
	class Integer(TypeEngine)
	
	class SmallInteger(Integer)
	
	class Numeric(TypeEngine): 
	    def __init__(self, precision=10, length=2)
	    
	class Float(Numeric):
	    def __init__(self, precision=10)
	    
	class DateTime(TypeEngine)
	
	class Date(TypeEngine)
	
	class Time(TypeEngine)
	    
	class Binary(TypeEngine): 
	    def __init__(self, length=None)
	    
	class Boolean(TypeEngine)

	# converts unicode strings to raw bytes
	# as bind params, raw bytes to unicode as 
	# rowset values, using the unicode encoding 
	# setting on the engine (defaults to 'utf-8')
	class Unicode(String)
	
	# uses the pickle protocol to serialize data
	# in/out of Binary columns
	class PickleType(Binary)
</&>
<p>More specific subclasses of these types are available, which various database engines may choose to implement specifically, allowing finer grained control over types:</p>
<&|formatting.myt:code&>
class FLOAT(Numeric)
class TEXT(String)
class DECIMAL(Numeric)
class INT(Integer)
INTEGER = INT
class TIMESTAMP(DateTime)
class DATETIME(DateTime)
class CLOB(String)
class VARCHAR(String)
class CHAR(String)
class BLOB(Binary)
class BOOLEAN(Boolean)
</&>
<p>When using a specific database engine, these types are adapted even further via a set of database-specific subclasses defined by the database engine.</p>
<p>There may eventually be more type objects that are defined for specific databases.  An example of this would be Postgres' Array type.
</p>
<p>Type objects are specified to table meta data using either the class itself, or an instance of the class.  Creating an instance of the class allows you to specify parameters for the type, such as string length, numerical precision, etc.:</p>
<&|formatting.myt:code&>
    mytable = Table('mytable', engine, 
        # define type using a class
        Column('my_id', Integer, primary_key=True), 

        # define type using an object instance
        Column('value', Number(7,4)) 
    )
</&>
</&>

<&|doclib.myt:item, name="custom", description="Creating your Own Types" &>
<p>User-defined types can be created, to support either database-specific types, or customized pre-processing of query parameters as well as post-processing of result set data.  You can make your own classes to perform these operations.  They are specified by subclassing the desired type class:</p>
<&|formatting.myt:code, title="Basic Example"&>
    import sqlalchemy.types as types

    class MyType(types.String):
        """basic type that decorates String, prefixes values with "PREFIX:" on 
        the way in and strips it off on the way out."""
        def convert_bind_param(self, value, engine):
            return "PREFIX:" + value
        def convert_result_value(self, value, engine):
            return value[7:]
</&>
<p>A common desire is for a "pickle" type, which overrides a Binary object to provide pickling behavior:
<&|formatting.myt:code, title="Pickle Type"&>
    import cPickle

    class PickleType(Binary):
          def __init__(self, protocol=pickle.HIGHEST_PROTOCOL):
               """allows the pickle protocol to be specified"""
               self.protocol = protocol
          def convert_result_value(self, value, engine):
              if value is None:
                  return None
              buf = Binary.convert_result_value(self, value, engine)
              return pickle.loads(str(buf))
          def convert_bind_param(self, value, engine):
              if value is None:
                  return None
              return Binary.convert_bind_param(self, pickle.dumps(value, self.protocol), engine)
          def get_constructor_args(self):
                return {}
</&>
<p>Which can be used like:</p>
<&|formatting.myt:code&>
    mytable = Table('mytable', engine, 
            Column('id', Integer, primary_key=True),
            Column('data', PickleType()))
            
    my_object = MyObject()
    mytable.insert().execute(data=my_object)
</&>
<p>Another example, which illustrates a fully defined datatype.  This just overrides the base type class TypeEngine:</p>
<&|formatting.myt:code&>
    import sqlalchemy.types as types

    class MyType(types.TypeEngine):
        def __init__(self, precision = 8):
            self.precision = precision
        def get_col_spec(self):
            return "MYTYPE(%s)" % self.precision
        def convert_bind_param(self, value, engine):
            return value
        def convert_result_value(self, value, engine):
            return value
        def adapt_args(self):
            """allows for the adaptation of this TypeEngine object into a new kind of type depending on its arguments."""
            return self
</&>
</&>
</&>
