#
# Authors:     TS
# Maintainers: TS
# Copyright:   2019, HRDAG, GPL v2 or later
# =========================================
# blocking/write/Makefile

.PHONY: all clean

all: output/greedy-blocking.html output/greedy-blocking.md

clean: 
		-rm -r output/*

output/greedy-blocking.html output/greedy-blocking.md: \
		src/greedy-blocking.Rmd
	Rscript -e "rmarkdown::render('$<', output_format = 'all')"
	mv src/greedy-blocking.html output/greedy-blocking.html
	mv src/greedy-blocking.md output/greedy-blocking.md
	-rm src/greedy-blocking.md
	-rm src/greedy-blocking.html

# done.
