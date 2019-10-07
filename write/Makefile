#
# Authors:     TS
# Maintainers: TS
# Copyright:   2019, HRDAG, GPL v2 or later
# =========================================
# blocking/write/Makefile

.PHONY: all clean

all: output/greedy-blocking.html

clean: 
		-rm -r output/*

output/greedy-blocking.html: \
		src/greedy-blocking.Rmd
	Rscript -e "rmarkdown::render('$<')"
	mv src/greedy-blocking.html output/greedy-blocking.html
	-rm src/greedy-blocking.md
	-rm src/greedy-blocking.html

# done.
