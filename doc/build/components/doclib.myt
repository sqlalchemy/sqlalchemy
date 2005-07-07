
<%python scope="global">
import sys, string, re

# datastructure that will store the whole contents of the documentation
class TOCElement:
	def __init__(self, filename, name, description, parent = None, ext = None, header = None, last_updated = 0):
		self.filename = filename
		self.name = name
		self.parent = parent
		self.path = self._create_path()
		self.header = header

		if self.parent is not None:
			self.root = parent.root
			self.root.pathlookup[self.path] = self

			if self.parent.filename != self.filename:
				self.root.filelookup[self.filename] = self
				self.isTop = True
		else:
			self.root = self
			self.pathlookup = {}
			self.pathlookup[''] = self
			self.filelookup = {}
			self.filelookup[filename] = self

		if ext is not None:
			self.ext = ext
		else:
			self.ext = self.root.ext

		self.last_updated = last_updated	
		self.description = description
		self.content = None
		self.previous = None
		self.next = None
		self.children = []
		if parent:
			if len(parent.children):
				self.previous = parent.children[-1]
				parent.children[-1].next = self
			parent.children.append(self)
			if last_updated > parent.last_updated:
				parent.last_updated = self.last_updated

	def get_file(self, name):
		name = re.sub("\.\w+$", "", name)
		return self.root.filelookup[name]

	def lookup(self, path):
		return self.root.pathlookup[path]

	def get_link(self, includefile = True, anchor = True):
		if includefile:
			if anchor:
				return "%s%s#%s" % (self.filename, self.ext, self.path)	
			else:
				return "%s%s" % (self.filename, self.ext)
		else:
			if anchor:
				return "#" + self.path	
			else:
				return ""

		
	def _create_path(self):
		elem = self
		tokens = []
		while elem.parent is not None:
			tokens.insert(0, elem.name)
			elem = elem.parent
		path = string.join(tokens, '_')
		return path



</%python>

<%python scope="request">
	current = Value()
	filename = Value()
</%python>


<%args scope="request">
	paged = 'yes'
</%args>

<%python scope="init">

	try:
		a = r
		isdynamic = True
		ext = ".myt"
	except:
		isdynamic = False
		ext = ".html"
		
	request_comp = m.request_comp()

	if isdynamic and not m.interpreter.attributes.get('docs_static_cache', False):
		page_cache = True
	else:
		page_cache = False

	# for dynamic page, cache the output of the final page
	
	if page_cache:
		if m.cache_self(key="doc_%s" % paged, component = request_comp):
			return

	list_comp = m.fetch_next()
	files = request_comp.attributes['files']
	title = request_comp.attributes.setdefault('title', "Documentation")
	version = request_comp.attributes['version']
	wrapper = request_comp.attributes['wrapper']
	index = request_comp.attributes['index']
	onepage = request_comp.attributes['onepage']



	def buildtoc():
		root = TOCElement("", "root", "root element", ext = ext)
		current.assign(root) 
	
		for file in files:
			filename.assign(file)
			comp = m.fetch_component(file + ".myt")
		
			main = m.scomp(comp)

		return root

	if not page_cache:
		# non-dynamic (i.e. command-line) page, cache the datastructure so successive
		# pages are fast (disables auto-recompiling)
		cache = m.get_cache(list_comp)

		toc = cache.get_value('toc', createfunc = buildtoc)
	
	else:
		toc = buildtoc()

	last_updated = toc.last_updated
	m.comp(wrapper, isdynamic=isdynamic, ext = ext, toc = toc, comp = request_comp, onepage = onepage, paged = paged, title = title, version = version, index=index, last_updated = last_updated)

</%python>

<%method title>
<% m.request_comp().get_attribute('title', inherit = True) or "Documentation" %>
</%method>

<%method item>
	<%doc>stores an item in the table of contents</%doc>
	<%args>
		# name should be a URL friendly name used for hyperlinking the section
		name

		# description is the heading for the item
		description

		escapedesc = False
		
		header = None
	</%args>
	<%python scope="init">
		if escapedesc:
			description = m.apply_escapes(description, ['h'])

 		current(TOCElement(filename(), name, description, current(), header = header, last_updated = m.caller.component_source.last_modified))
 		current().content = m.content()
 		current(current().parent)
 </%python></%method>


<%method current>
<%init>return current()</%init>
</%method>





