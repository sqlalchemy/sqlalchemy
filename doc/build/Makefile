# Makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
PAPER         =

# Internal variables.
PAPEROPT_a4     = -D latex_paper_size=a4
PAPEROPT_letter = -D latex_paper_size=letter
ALLSPHINXOPTS   = -d output/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) .

.PHONY: help clean html latex site-mako

help:
	@echo "Please use \`make <target>' where <target> is one of"
	@echo "  html      to make standalone HTML files"
	@echo "  dist-html same as html, but places files in /doc"
	@echo "  latex     to make LaTeX files, you can set PAPER=a4 or PAPER=letter"

clean:
	-rm -rf output/*

html:
	mkdir -p output/html output/doctrees
	$(SPHINXBUILD) -b html -A mako_layout=html $(ALLSPHINXOPTS) output/html
	@echo
	@echo "Build finished. The HTML pages are in output/html."

dist-html:
	$(SPHINXBUILD) -b html -A mako_layout=html $(ALLSPHINXOPTS) ..
	@echo
	@echo "Build finished.  The HTML pages are in ../."

site-mako:
	mkdir -p output/site output/doctrees
	$(SPHINXBUILD) -b html -A mako_layout=site $(ALLSPHINXOPTS) output/site
	@echo
	@echo "Build finished. The Mako pages are in output/site."

latex:
	mkdir -p output/latex output/doctrees
	$(SPHINXBUILD) -b latex $(ALLSPHINXOPTS) output/latex
	cp texinputs/* output/latex/
	@echo
	@echo "Build finished; the LaTeX files are in output/latex."
	@echo "Run \`make all-pdf' or \`make all-ps' in that directory to" \
	      "run these through (pdf)latex."
doctest:
	$(SPHINXBUILD) -b doctest -d output/doctrees . .
