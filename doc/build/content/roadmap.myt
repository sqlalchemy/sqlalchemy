<%flags>inherit='document_base.myt'</%flags>
<&|doclib.myt:item, name="roadmap", description="Roadmap" &>
<p>SQLAlchemy includes several components, each of which are useful by themselves to give varying levels of assistance to a database-enabled application.  Below is a roadmap of the "knowledge dependencies" between these components indicating the order in which concepts may be learned.  
</p>

<pre>
Start
  |
  |
  |--- Establishing Transparent Connection Pooling
  |              |
  |              |
  |              |------ Connection Pooling Configuration
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
                                   |---- <&formatting.myt:link, path="metadata_building" &>
                                   | 
                                   |    
                                   |---- <&formatting.myt:link, path="sql" &>
                                   |                                      |                
                                   |                                      |                                  
                                   |---- Basic Data Mapping               |                
                                   |               |                      |  
                                   |               |                      |              
                                   |               +----------- Advanced Data Mapping
                                   |                                        
                                   |                
                                   +----- Basic Active Record
</pre>
</&>