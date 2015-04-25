#
# Makefile for generating documents
# 
# gxpman.pdf
# gxpman.info
# gxpman.html
#
# distribution contains them so you normally have to run this.
#
# to generate them, you need
# (1) makeinfo (install texinfo package).
# (2) texinfo.tex in this directory (install texinfo package and then
#     copy texinfo.tex here).
# (3) dvipdfm (for gxpman.pdf)
#



M = gxpman

all : $(M).pdf $(M).info $(M).html 

cmdref.tex : ../gxpc ../gxpc.py
	../gxpc makeman > cmdref.tex

$(M).tex : $(M)_src.tex cmdref.tex
	python include.py $< tmp.tex
	emacs -q -batch --load=make_texinfo.el

$(M).info : $(M).tex
	makeinfo --fill-column=70 $(M).tex

$(M).dvi : $(M).tex
	tex $(M).tex

$(M).pdf : $(M).dvi
	dvipdfm $(M).dvi

$(M).html : $(M).tex
	makeinfo --no-split --force --html --fill-column=70 --output tmp.html $< ; python postprocess_html.py tmp.html $@

clean :
	rm -f $(M).aux $(M).info $(M).log $(M).cp $(M).info-1 $(M).md 
	rm -f $(M).toc $(M).info-2 $(M).pg $(M).tp $(M).fn $(M).ky $(M).vr
	rm -f cmdref.tex tmp.tex tmp.html
