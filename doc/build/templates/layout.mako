## coding: utf-8

<%!
    local_script_files = []

    default_css_files = [
        '_static/pygments.css',
        '_static/docs.css',
    ]
%>


<%doc>
    Structural elements are all prefixed with "docs-"
    to prevent conflicts when the structure is integrated into the
    main site.

    docs-container ->
        docs-top-navigation-container ->
            docs-header ->
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
    if builder == 'epub':
        next.body()
        return
%>


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

    ${parent.headers()}

    <!-- begin layout.mako headers -->

    <script type="text/javascript">
      var DOCUMENTATION_OPTIONS = {
          URL_ROOT:    '${pathto("", 1)}',
          VERSION:     '${release|h}',
          COLLAPSE_MODINDEX: false,
          FILE_SUFFIX: '${file_suffix}'
      };
    </script>

    <!-- begin iterate through sphinx environment script_files -->
    % for scriptfile in script_files + self.attr.local_script_files:
        <script type="text/javascript" src="${pathto(scriptfile, 1)}"></script>
    % endfor
    <!-- end iterate through sphinx environment script_files -->

    <script type="text/javascript" src="${pathto('_static/detectmobile.js', 1)}"></script>
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
    <!-- end layout.mako headers -->

</%block>


<div id="docs-top-navigation-container" class="body-background">
<div id="docs-header">
    <div id="docs-version-header">
        Release: <span class="version-num">${release}</span> | Release Date: ${release_date}
    </div>

    <h1>${docstitle|h}</h1>

</div>
</div>

<div id="docs-body-container">

    <div id="fixed-sidebar" class="${'withsidebar' if withsidebar else ''}">

    % if not withsidebar:
        <div id="index-nav">
            <form class="search" action="${pathto('search')}" method="get">
              <input type="text" name="q" size="12" /> <input type="submit" value="${_('Search')}" />
              <input type="hidden" name="check_keywords" value="yes" />
              <input type="hidden" name="area" value="default" />
            </form>

            <p>
            <a href="${pathto('index')}">Contents</a> |
            <a href="${pathto('genindex')}">Index</a>
            % if pdf_url:
            | <a href="${pdf_url}">Download as PDF</a>
            % endif
            </p>

        </div>
    % endif

    % if withsidebar:
        <div id="docs-sidebar-popout">
            <h3><a href="${pathto('index')}">${docstitle|h}</a></h3>

            <p id="sidebar-paginate">
                % if parents:
                    <a href="${parents[-1]['link']|h}" title="${parents[-1]['title']}">Up</a> |
                % else:
                    <a href="${pathto('index')}" title="${docstitle|h}">Up</a> |
                % endif

                % if prevtopic:
                    <a href="${prevtopic['link']|h}" title="${prevtopic['title']}">Prev</a> |
                % endif
                % if nexttopic:
                    <a href="${nexttopic['link']|h}" title="${nexttopic['title']}">Next</a>
                % endif
            </p>

            <p id="sidebar-topnav">
                <a href="${pathto('index')}">Contents</a> |
                <a href="${pathto('genindex')}">Index</a>
                % if pdf_url:
                | <a href="${pdf_url}">PDF</a>
                % endif
            </p>

            <div id="sidebar-search">
                <form class="search" action="${pathto('search')}" method="get">
                  <input type="text" name="q" size="12" /> <input type="submit" value="${_('Search')}" />
                  <input type="hidden" name="check_keywords" value="yes" />
                  <input type="hidden" name="area" value="default" />
                </form>
            </div>

        </div>

        <div id="docs-sidebar">

        <h3><a href="#">\
            <%block name="show_title">
                ${title}
            </%block>
        </a></h3>
        ${toc}

        % if rtd:
        <h4>Project Versions</h4>
        <ul class="version-listing">
        </ul>
        % endif


        </div>
    % endif

    </div>

    <%doc>
    <div id="docs-top-navigation">
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
    </%doc>

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
