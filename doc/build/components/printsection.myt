<%args>
	toc
	paged
	comp
	isdynamic
	index
	ext
	onepage
</%args>

<%python scope="init">
	# get the item being requested by this embedded component from the documentation tree
	try:
		current = toc.get_file(comp.get_name())
	except KeyError:
		current = None
</%python>

% if paged == 'yes':
%	if current is None:
		<& toc, includefile = True, **ARGS &>
% 	else:
		<A name="<% current.path %>"></a>
		<& topnav, item=current, **ARGS &>
		<div class="sectioncontent">
			<& formatting.myt:printitem, item=current, includefile = True, omitheader = True &>
		</div>
% 	# if/else
% else:
	<& toc, includefile = False, **ARGS &>
	<div class="onepagecontent">
%	for i in toc.children:
		<div class="section">
					
			<A name="<% i.path %>"></a>
			<div class="topnavmargin">
			<& topnav, item=i, **ARGS &>
			</div>

			<& formatting.myt:printitem, item=i, includefile = False, omitheader = True &>
		</div>
%	# for i
	</div>
	
% # if/else


<%method topnav>
	<%args>
		isdynamic
		paged
		item
		index
		ext
		onepage
	</%args>
% ispaged = (paged =='yes')

<div class="topnav">

<& topnavcontrol, **ARGS &>

<div class="topnavsectionlink">

<a href="<% ispaged and 'index' + ext or '#top' %>">Table of Contents</a>

<div class="prevnext">
% if item.previous is not None:
Previous: <& formatting.myt:itemlink, item=item.previous, includefile=ispaged &>
%

% if item.previous is not None and item.next is not None:
&nbsp; | &nbsp;
%

% if item.next is not None:

Next: <& formatting.myt:itemlink, item=item.next, includefile=ispaged &>
%

</div>
</div>


<div class="topnavmain">
	<div class="topnavheader"><% item.description %></div>
	<div class="topnavitems">
	<& formatting.myt:printtoc, root = item, includefile = False, current = None, full = True &>
	</div>
</div>

</div>
</%method>

<%method topnavcontrol>
	<%args>
		isdynamic
		paged
		index
		ext
		onepage
	</%args>
% ispaged = (paged =='yes')

	<div class="topnavcontrol">
% if ispaged:
	View: <b>Paged</b> &nbsp;|&nbsp; <a href="<% isdynamic and index + ext + '?paged=no' or onepage + ext %>">One Page</a>
% else:
	View: <a href="<% index + ext %>">Paged</a> &nbsp;|&nbsp; <b>One Page</b>
%
	</div>

</%method>

<%method toc>
	<%args>
		toc
		includefile = True
		isdynamic
		paged
		index
		ext
		onepage
	</%args>
	
	
	<div class="maintoc">
	<& topnavcontrol, **ARGS &>

	<a name="table_of_contents"></a>
	<span class="docheadertext">Table of Contents</span>
	&nbsp;&nbsp;
	<a href="#full_index">(view full table)</a>
	<br/><br/>
	
	<div style="margin-left:50px;">
	<& formatting.myt:printtoc, root = toc, includefile = includefile, current = None, full = False, children=False &>
	</div>

	</div>


	<div class="maintoc">
	<a name="full_index"></a>
	<span class="docheadertext">Table of Contents: Full</span>
	&nbsp;&nbsp;
	<a href="#table_of_contents">(view brief table)</a>
	<br/><br/>

	<div style="margin-left:50px;">
	<& formatting.myt:printtoc, root = toc, includefile = includefile, current = None, full = True, children=True &>
	</div>

	</div>
</%method>
