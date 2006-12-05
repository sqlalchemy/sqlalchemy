<%doc>nav.myt - Provides page navigation elements that are derived from toc.TOCElement structures, including
individual hyperlinks as well as navigational toolbars and table-of-content listings.</%doc>

<%method itemlink trim="both">
    <%args>
    item
    anchor=True
    usefilename=True
    </%args>
    <%args scope="request">
        extension='myt'
    </%args>
    <a href="<% item.get_link(extension=extension, anchor=anchor, usefilename=usefilename) %>"><% item.description %></a>
</%method>

<%method toclink trim="both">
    <%args>
        toc 
        path
        description=None
        extension
        usefilename=True
    </%args>
    <%init>
        item = toc.get_by_path(path)
        if description is None:
            if item:
                description = item.description
            else:
                description = path
    </%init>
% if item:
    <a href="<% item.get_link(extension=extension, usefilename=usefilename) %>"><% description %></a>
% else:
    <b><% description %></b>
%
</%method>


<%method link trim="both">
    <%args>
        href
        text
        class_
    </%args>
    <a href="<% href %>" <% class_ and (('class=\"%s\"' % class_) or '')%>><% text %></a>
</%method>

<%method topnav>
	<%args>
		item
		extension
		onepage=False
	</%args>
<div class="topnav">

<div class="topnavsectionlink">

<a href="index.<% extension %>">Table of Contents</a>

<div class="prevnext">
% if item.previous is not None:
Previous: <& itemlink, item=item.previous, anchor=False &>
%

% if item.previous is not None and item.next is not None:
&nbsp; | &nbsp;
%

% if item.next is not None:

Next: <& itemlink, item=item.next, anchor=False &>
%

</div>
</div>

<div class="topnavmain">
	<div class="topnavheader"><% item.description %></div>
	<div class="topnavitems">
	<& toc.myt:printtoc, root=item, current=None, full=True, extension=extension, anchor_toplevel=True, onepage=False &>
	</div>
</div>

</div>
</%method>

<%method pagenav>
<%args>
    item
    onepage=False
</%args>
<div class="sectionnavblock">
<div class="sectionnav">

%       if item.previous is not None:
        Previous: <& itemlink, item=item.previous, usefilename=not onepage &>
%       # end if

%       if item.next is not None:
%               if item.previous is not None:
                |
%               # end if

        Next: <& itemlink, item=item.next, usefilename=not onepage &>
%       # end if

</div>
</div>
</%method>
