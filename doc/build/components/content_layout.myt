<%doc>defines the default layout for normal documentation pages (not including the index)</%doc>
<%args>
    extension="myt"
    toc
</%args>
<%flags>inherit="base.myt"</%flags>
<%init>
    current = toc.get_by_file(m.request_component.attributes['filename'])
</%init>

<A name="<% current.path %>"></a>
<& nav.myt:topnav, item=current, extension=extension, onepage=True &>
<div class="sectioncontent">
% m.call_next(toc=toc, extension=extension)
</div>
