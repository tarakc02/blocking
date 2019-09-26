#
# Authors:     TS
# Maintainers: TS
# Copyright:   2019, HRDAG, GPL v2 or later
# =========================================
# blocking/block/Makefile

.PHONY: all clean

all: output/small.txt output/medium.txt output/large.txt

clean:
		-rm -r output/*

output/small.txt: \
		src/find-rules.jl \
		input/small-recs.feather \
		input/small-pairs.feather
	julia $< input/small-recs.feather input/small-pairs.feather 5000 > $@

output/medium.txt: \
		src/find-rules.jl \
		input/medium-recs.feather \
		input/medium-pairs.feather
	julia $< input/medium-recs.feather input/medium-pairs.feather 50000 > $@

output/large.txt: \
		src/find-rules.jl \
		input/large-recs.feather \
		input/large-pairs.feather
	julia $< input/large-recs.feather input/large-pairs.feather 1000000 > $@

