<%text>#coding:utf-8
<%inherit file="/base.html"/>
<%page cache_type="file" cached="True"/>
<%!
    in_docs=True
%>
</%text>

<div style="text-align:right">
<b>Jump to Version:</b> <a href="/docs/07/">0.7</a> | <a href="/docs/06/">0.6</a> | <a href="/docs/05/">0.5</a>
</div>

<div style="text-align:right">
<b>PDF Download:</b> <a href="${pathto('sqlalchemy_' + release.replace('.', '_') + '.pdf', 1)}">download</a>
</div>

${'<%text>'}
${next.body()}
${'</%text>'}

<%text><%def name="style()"></%text>
    <%block name="headers"/>

    <%text>${parent.style()}</%text>
    <link href="/css/site_docs.css" rel="stylesheet" type="text/css"></link>
<%text></%def></%text>

<%text><%def name="title()"></%text>${capture(self.show_title)|util.striptags} &mdash; ${docstitle|h}<%text></%def></%text>

<%!
    local_script_files = []
%>
