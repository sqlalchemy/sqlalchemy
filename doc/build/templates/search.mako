<%inherit file="layout.mako"/>

<%!
    local_script_files = ['_static/searchtools.js']
%>
<%block name="show_title">
    ${_('Search')}
</%block>

<%block name="headers">
    ${parent.headers()}
    <script type="text/javascript">
        jQuery(function() { Search.loadIndex("searchindex.js"); });
    </script>
</%block>

<div id="search-results"></div>

<%block name="footer">
    ${parent.footer()}
</%block>
