<%flags>inherit='base.myt'</%flags>
<%args>
    extension
    toc
    onepage=False
</%args>

<a href="index.<% extension %>">Multiple Pages</a> | 
<a href="documentation.<% extension %>">One Page</a><br/>
<& toc.myt:toc, toc=toc, extension=extension, onepage=onepage &>
