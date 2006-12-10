<%flags>inherit='base.myt'</%flags>
<%args>
    extension
    toc
</%args>
<%method title>
    SQLAlchemy Documentation
</%method>
<& index.myt, toc=toc, extension=extension, onepage=True &>
% for file in toc.filenames:
% current = toc.get_by_file(file)
<A name="<% current.path %>"></a>
<& nav.myt:pagenav, item=current, extension=extension, onepage=True &>
<div class="topnavmain">
	<div class="topnavheader"><% current.description %></div>
</div>
<div class="sectioncontent">
%   m.comp(file + ".myt", toc=toc, extension=extension, onepage=True)
</div>
%
