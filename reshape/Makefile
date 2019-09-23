#
# Authors:     TS
# Maintainers: TS
# Copyright:   2019, HRDAG, GPL v2 or later
# =========================================
# blocking/reshape/Makefile

.PHONY: all clean

recs := output/small-recs.feather \
	output/medium-recs.feather \
	output/large-recs.feather

pairs := output/small-pairs.feather \
	output/medium-pairs.feather \
	output/large-pairs.feather

all: $(recs) $(pairs)

clean: 
		-rm -r output/*

output/small-recs.feather: input/small.feather
	cd output && ln -s ../input/small.feather small-recs.feather

output/medium-recs.feather: input/medium.feather
	cd output && ln -s ../input/medium.feather medium-recs.feather

output/large-recs.feather: input/large.feather
	cd output && ln -s ../input/large.feather large-recs.feather

output/%-pairs.feather: \
		src/prep-data.R \
		$(recs)
	Rscript --vanilla $< \
			--input=input/$*.feather \
			--output=$@

# done.
