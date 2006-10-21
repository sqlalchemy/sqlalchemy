<%doc>pydoc.myt - provides formatting functions for printing docstring.AbstractDoc generated python documentation objects.</%doc>

<%global>
import docstring
</%global>

<%method obj_doc>
    <%args>
        obj
        toc
        extension
    </%args>
<%init>
if obj.isclass:
    links = []
    for elem in obj.inherits:
        if isinstance(elem, docstring.ObjectDoc):
            links.append(m.scomp("nav.myt:toclink", toc=toc, path=elem.toc_path, extension=extension, description=elem.name))
        else:
            links.append(str(elem))
    htmldescription = "class " + obj.classname + "(%s)" % (','.join(links))
else:
    htmldescription = obj.description

</%init>

<&|formatting.myt:section, toc=toc, path=obj.toc_path, description=htmldescription &>

<&|formatting.myt:formatplain&><% obj.doc %></&>

% if not obj.isclass and obj.functions:

%   for func in obj.functions:
    <& SELF:function_doc, func=func &>
%

% else:

% if obj.functions:
%   for func in obj.functions:
%   if isinstance(func, docstring.FunctionDoc):
    <& SELF:function_doc, func=func &>
%   elif isinstance(func, docstring.PropertyDoc):
    <& SELF:property_doc, prop=func &>
%
%
%
%

% if obj.classes:
%   for class_ in obj.classes:
      <& SELF:obj_doc, obj=class_, toc=toc, extension=extension &>
%   
%    
</&>
</%method>

<%method function_doc>
    <%args>func</%args>
        <div class="darkcell">
        <A name=""></a>
        <b><% func.name %>(<% ", ".join(map(lambda k: "<i>%s</i>" % k, func.arglist))%>)</b>
        <div class="docstring">
        <&|formatting.myt:formatplain&><% func.doc %></&>
        </div>
        </div>
</%method>


<%method property_doc>
    <%args>
        prop
    </%args>
         <div class="darkcell">
         <A name=""></a>
         <b><% prop.name %></b>
         <div class="docstring">
         <&|formatting.myt:formatplain&><% prop.doc %></&>
         </div> 
         </div>
</%method>


