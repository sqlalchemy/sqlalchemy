<%global>
    import docstring, string, sys
</%global>

<%method obj_doc>
    <%args>
        obj
    </%args>

<%python>
if obj.isclass:
    s = []
    links = []
    for elem in obj.inherits:
        if isinstance(elem, docstring.ObjectDoc):
            links.append("<a href=\"#%s\">%s</a>" % (str(elem.id), elem.name))
            s.append(elem.name)
        else:
            links.append(str(elem))
            s.append(str(elem))
    description = "class " + obj.classname + "(%s)" % (','.join(s))
    htmldescription = "class " + obj.classname + "(%s)" % (','.join(links))
else:
    description = obj.description
    htmldescription = obj.description
    
</%python>
<&|doclib.myt:item, name=obj.name, description=description, htmldescription=htmldescription, altlink=str(obj.id) &>
<&|formatting.myt:formatplain&><% obj.doc %></&>

% if not obj.isclass and obj.functions:
<&|doclib.myt:item, name="modfunc", description="Module Functions" &>
<&|formatting.myt:paramtable&>
%   for func in obj.functions:
    <& SELF:function_doc, func=func &>
%
</&>
</&>
% else:
% if obj.functions:
<&|formatting.myt:paramtable&>
%   for func in obj.functions:
%   if isinstance(func, docstring.FunctionDoc):
    <& SELF:function_doc, func=func &>
%   elif isinstance(func, docstring.PropertyDoc):
    <& SELF:property_doc, prop=func &>
%
%
</&>
%
%

% if obj.classes:
<&|formatting.myt:paramtable&>
%   for class_ in obj.classes:
      <& SELF:obj_doc, obj=class_ &>
%   
</&>
%    
</&>

</%method>

<%method function_doc>
    <%args>func</%args>
    <&|formatting.myt:function_doc, name=func.name, link=func.link, arglist=func.arglist &>
    <&|formatting.myt:formatplain&><% func.doc %></&>
    </&>
</%method>


<%method property_doc>
    <%args>
        prop
    </%args>
    <&|formatting.myt:member_doc, name=prop.name, link=prop.link &>
    <&|formatting.myt:formatplain&><% prop.doc %></&>
    </&>    
</%method>
