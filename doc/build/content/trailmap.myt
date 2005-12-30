<%flags>inherit='document_base.myt'</%flags>
<%attr>title='Introduction'</%attr>
<&|doclib.myt:item, name="intro", description="Introduction" &>
<p>SQLAlchemy features a lot of tools and patterns to help in every area of writing applications that talk to relational databases.  To achieve this, it has a lot of areas of functionality which work together to provide a cohesive package.  Ultimately, just a little bit of familiarity with each concept is all thats needed to get off the ground.</p>

<p>That said, here's two quick links that summarize the two most prominent features of SQLAlchemy:
<ul>
	<li><&formatting.myt:link, path="datamapping", class_="trailbold"&> - a synopsis of how to map objects to database tables (Object Relational Mapping)</li>
	<li><&formatting.myt:link, path="sql", class_="trailbold"&> - SQLAlchemy's own domain-oriented approach to constructing and executing SQL statements.</li>
</ul>
</p>

<&|doclib.myt:item, name="trailmap", description="Trail Map" &>
<p>For a comprehensive tour through all of SQLAlchemy's components, below is a "Trail Map" of the knowledge dependencies between these components indicating the order in which concepts may be learned.   Concepts marked in bold indicate features that are useful on their own.
</p>
<pre>
Start
  |
  |
  |--- <&formatting.myt:link, class_="trailbold", path="pooling" &>
  |              |
  |              |
  |              |------ <&formatting.myt:link, path="pooling_configuration" &>
  |                                         |              
  |                                         |
  +--- <&formatting.myt:link, path="dbengine_establishing" &>       |
                   |                        |
                   |                        | 
                   |--------- <&formatting.myt:link, path="dbengine_options" &>
                   |
                   |
                   +---- <&formatting.myt:link, path="metadata_tables" &>
                                   |
                                   |
                                   |---- <&formatting.myt:link, path="metadata_creating" &>
                                   | 
                                   |    
                                   |---- <&formatting.myt:link, path="sql", class_="trailbold" &>
                                   |                                      |                
                                   |                                      |                                  
                                   +---- <&formatting.myt:link, path="datamapping", class_="trailbold"&>               |                
                                   |               |                      |  
                                   |               |                      |  
                                   |         <&formatting.myt:link, path="unitofwork"&>                 |              
                                   |               |                      |              
                                   |               |                      |              
                                   |               +----------- <&formatting.myt:link, path="adv_datamapping"&>
                                   |                                       
                                   +----- <&formatting.myt:link, path="types"&>
</pre>
</&>
</&>
