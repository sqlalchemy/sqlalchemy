<%inherit file="layout.mako"/>

<%block name="show_title" filter="util.striptags">
    ${_('Index')}
</%block>

   <h1 id="index">${_('Index')}</h1>

   % for i, (key, dummy) in enumerate(genindexentries):
    ${i != 0 and '| ' or ''}<a href="#${key}"><strong>${key}</strong></a>
   % endfor

   <hr />

   % for i, (key, entries) in enumerate(genindexentries):
<h2 id="${key}">${key}</h2>
<table width="100%" class="indextable genindextable"><tr><td width="33%" valign="top">
<dl>
    <%
        breakat = genindexcounts[i] // 2
        numcols = 1
        numitems = 0
    %>
% for entryname, (links, subitems) in entries:

<dt>
    % if links:
        <a href="${links[0][1]}">${entryname|h}</a>
        % for unknown, link in links[1:]:
            , <a href="${link}">[${i}]</a>
        % endfor
    % else:
        ${entryname|h}
    % endif
</dt>

    % if subitems:
    <dd><dl>
      % for subentryname, subentrylinks in subitems:
      <dt><a href="${subentrylinks[0][1]}">${subentryname|h}</a>
              % for j, (unknown, link) in enumerate(subentrylinks[1:]):
                  <a href="${link}">[${j}]</a>
              % endfor
      </dt>
      % endfor
    </dl></dd>
    % endif

  <%
    numitems = numitems + 1 + len(subitems)
  %>
  % if numcols <2 and numitems > breakat:
     <%
        numcols = numcols + 1
     %>
        </dl></td><td width="33%" valign="top"><dl>
  % endif

% endfor
<dt></dt></dl>
</td></tr></table>
% endfor

<%def name="sidebarrel()">
% if split_index:
   <h4>${_('Index')}</h4>
   <p>
   % for i, (key, dummy) in enumerate(genindexentries):
       ${i > 0 and '| ' or ''}
       <a href="${pathto('genindex-' + key)}"><strong>${key}</strong></a>
   % endfor
   </p>

   <p><a href="${pathto('genindex-all')}"><strong>${_('Full index on one page')}</strong></a></p>
% endif
   ${parent.sidebarrel()}
</%def>
