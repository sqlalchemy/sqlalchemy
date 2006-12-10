<%doc>base.myt - common to all documentation pages. intentionally separate from autohandler, which can be swapped
out for a different one</%doc>
<%args>
    extension="myt"
</%args>
<%python scope="init">
    if m.cache_self(key=m.request_component.file):
        return
    # bootstrap TOC structure from request args, or pickled file if not present.
    import cPickle as pickle
    import os, time
    m.log("base.myt generating from table of contents for file %s" % m.request_component.file)
    toc = m.request_args.get('toc')
    if toc is None:
        filename = os.path.join(os.path.dirname(m.request_component.file), 'table_of_contents.pickle')
        toc = pickle.load(file(filename))
    version = toc.version
    last_updated = toc.last_updated
</%python>
<%method title>
    <% m.request_component.attributes.get('title') %>
</%method>

<div style="position:absolute;left:0px;top:0px;"><a name="top"></a>&nbsp;</div>

<div class="doccontainer">

<div class="docheader">


<h1><% toc.root.doctitle %></h1>

<div class="multipage">
<a href="index.<% extension %>">Multiple Pages</a> |
<a href="documentation.<% extension %>">One Page</a>
</div>

<div class="">Version: <% version %>   Last Updated: <% time.strftime('%x %X', time.localtime(last_updated)) %></div>
</div>

% m.call_next(toc=toc, extension=extension)

</div>


