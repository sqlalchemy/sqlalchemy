<%flags>inherit='document_base.myt'</%flags>
<&|doclib.myt:item, name="roadmap", description="Roadmap" &>
<p>SQLAlchemy includes several components, each of which are useful by themselves to give varying levels of assistance to a database-enabled application.  Below is a roadmap of the "knowledge dependencies" between these components indicating the order in which concepts may be learned.  
</p>

<pre>
Start
  |
  |
  |--- <&formatting.myt:link, path="pooling_establishing" &>
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
                                   |---- <&formatting.myt:link, path="sql" &>
                                   |                                      |                
                                   |                                      |                                  
                                   +---- <&formatting.myt:link, path="datamapping"&>               |                
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
