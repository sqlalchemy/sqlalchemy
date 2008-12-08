<%inherit file="layout.mako"/>

<%!
    local_script_files = ['_static/searchtools.js']
%>
<%def name="show_title()">${_('Search')}</%def>

<div id="searchform">
<h3>Enter Search Terms:</h3>
<form class="search" action="${pathto('search')}" method="get">
  <input type="text" name="q" size="18" /> <input type="submit" value="${_('Search')}" />
  <input type="hidden" name="check_keywords" value="yes" />
  <input type="hidden" name="area" value="default" />
</form>
</div>

<div id="search-results"></div>

<%def name="footer()">
    ${parent.footer()}
    <script type="text/javascript" src="searchindex.js"></script>
</%def>
