## coding: utf-8

<%!
    local_script_files = []
%>

<%doc>
    Structural elements are all prefixed with "docs-"
    to prevent conflicts when the structure is integrated into the 
    main site.
    
    docs-container ->
        docs-header ->
            docs-search
            docs-version-header
        docs-top-navigation
            docs-top-page-control
            docs-navigation-banner
        docs-body-container ->
            docs-sidebar
            docs-body
        docs-bottom-navigation
            docs-copyright
</%doc>

<%inherit file="${context['base']}"/>

<%
withsidebar = bool(toc) and current_page_name != 'index'
%>

<%block name="head_title">
    % if current_page_name != 'index':
    ${capture(self.show_title) | util.striptags} &mdash; 
    % endif
    ${docstitle|h}
</%block>

<div id="docs-container">

<%block name="headers">
    <link rel="stylesheet" href="${pathto('_static/pygments.css', 1)}" type="text/css" />
    <link rel="stylesheet" href="${pathto('_static/docs.css', 1)}" type="text/css" />

    <script type="text/javascript">
      var DOCUMENTATION_OPTIONS = {
          URL_ROOT:    '${pathto("", 1)}',
          VERSION:     '${release|h}',
          COLLAPSE_MODINDEX: false,
          FILE_SUFFIX: '${file_suffix}'
      };
    </script>
    % for scriptfile in script_files + self.attr.local_script_files:
        <script type="text/javascript" src="${pathto(scriptfile, 1)}"></script>
    % endfor
    <script type="text/javascript" src="${pathto('_static/init.js', 1)}"></script>
    % if hasdoc('about'):
        <link rel="author" title="${_('About these documents')}" href="${pathto('about')}" />
    % endif
    <link rel="index" title="${_('Index')}" href="${pathto('genindex')}" />
    <link rel="search" title="${_('Search')}" href="${pathto('search')}" />
    % if hasdoc('copyright'):
        <link rel="copyright" title="${_('Copyright')}" href="${pathto('copyright')}" />
    % endif
    <link rel="top" title="${docstitle|h}" href="${pathto('index')}" />
    % if parents:
        <link rel="up" title="${parents[-1]['title']|util.striptags}" href="${parents[-1]['link']|h}" />
    % endif
    % if nexttopic:
        <link rel="next" title="${nexttopic['title']|util.striptags}" href="${nexttopic['link']|h}" />
    % endif
    % if prevtopic:
        <link rel="prev" title="${prevtopic['title']|util.striptags}" href="${prevtopic['link']|h}" />
    % endif
</%block>

<div id="docs-header">
    <h1>${docstitle|h}</h1>

    <div id="docs-search">
    Search:
    <form class="search" action="${pathto('search')}" method="get">
      <input type="text" name="q" size="18" /> <input type="submit" value="${_('Search')}" />
      <input type="hidden" name="check_keywords" value="yes" />
      <input type="hidden" name="area" value="default" />
    </form>
    </div>

    <div id="docs-version-header">
        Release: <span class="version-num">${release}</span> | Release Date: ${release_date}

        % if pdf_url:
        | <a href="${pdf_url}">Download PDF</a>
        % endif

    </div>

</div>

<div id="docs-top-navigation">
    <div id="docs-top-page-control" class="docs-navigation-links">
        <ul>
        % if prevtopic:
            <li>Prev:
            <a href="${prevtopic['link']|h}" title="${_('previous chapter')}">${prevtopic['title']}</a>
            </li>
        % endif
        % if nexttopic:
            <li>Next:
            <a href="${nexttopic['link']|h}" title="${_('next chapter')}">${nexttopic['title']}</a>
            </li>
        % endif

        <li>
            <a href="${pathto('contents')}">Table of Contents</a> |
            <a href="${pathto('genindex')}">Index</a>
            % if sourcename:
            | <a href="${pathto('_sources/' + sourcename, True)|h}">${_('view source')}
            % endif
        </li>
        </ul>
    </div>

    <div id="docs-navigation-banner">
        <a href="${pathto('index')}">${docstitle|h}</a>
        % if parents:
            % for parent in parents:
                » <a href="${parent['link']|h}" title="${parent['title']}">${parent['title']}</a>
            % endfor
        % endif
        % if current_page_name != 'index':
        » ${self.show_title()} 
        % endif

        <h2>
            <%block name="show_title">
                ${title}
            </%block>
        </h2>
    </div>

</div>

<div id="docs-body-container">

% if withsidebar:
    <div id="docs-sidebar">
    <h3><a href="${pathto('index')}">Table of Contents</a></h3>
    ${toc}

    % if prevtopic:
    <h4>Previous Topic</h4>
    <p>
    <a href="${prevtopic['link']|h}" title="${_('previous chapter')}">${prevtopic['title']}</a>
    </p>
    % endif
    % if nexttopic:
    <h4>Next Topic</h4>
    <p>
    <a href="${nexttopic['link']|h}" title="${_('next chapter')}">${nexttopic['title']}</a>
    </p>
    % endif

    % if rtd:
    <h4>Project Versions</h4>
    <ul class="version-listing">
    </ul>
    % endif

    <h4>Quick Search</h4>
    <p>
    <form class="search" action="${pathto('search')}" method="get">
      <input type="text" name="q" size="18" /> <input type="submit" value="${_('Search')}" />
      <input type="hidden" name="check_keywords" value="yes" />
      <input type="hidden" name="area" value="default" />
    </form>
    </p>

    </div>
% endif

    <div id="docs-body" class="${'withsidebar' if withsidebar else ''}" >
        ${next.body()}
    </div>

</div>

<div id="docs-bottom-navigation" class="docs-navigation-links">
    % if prevtopic:
        Previous:
        <a href="${prevtopic['link']|h}" title="${_('previous chapter')}">${prevtopic['title']}</a>
    % endif
    % if nexttopic:
        Next:
        <a href="${nexttopic['link']|h}" title="${_('next chapter')}">${nexttopic['title']}</a>
    % endif

    <div id="docs-copyright">
    % if hasdoc('copyright'):
        &copy; <a href="${pathto('copyright')}">Copyright</a> ${copyright|h}.
    % else:
        &copy; Copyright ${copyright|h}.
    % endif
    % if show_sphinx:
        Created using <a href="http://sphinx.pocoo.org/">Sphinx</a> ${sphinx_version|h}.
    % endif
    </div>
</div>

</div>
