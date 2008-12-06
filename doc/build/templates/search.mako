<%inherit file="layout.mako"/>

<%!
    local_script_files = ['_static/searchtools.js']
%>
<%def name="show_title()">${_('Search')}</%def>

<div id="search-results"></div>

<%def name="footer()">
    ${parent.footer()}
    <script type="text/javascript" src="searchindex.js"></script>
</%def>
