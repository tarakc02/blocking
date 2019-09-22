#
# Authors:     TS
# Maintainers: TS
# Copyright:   2019, HRDAG, GPL v2 or later
# =========================================
# blocking/simulate/Makefile

.PHONY: all clean

all: output/small.feather output/medium.feather output/large.feather

clean: 
		-rm -r output/*

output/small.feather: src/generate.R
	Rscript --vanilla $< \
			--nrow=1000  \
			--ncol=10    \
			--npair=472  \
			--output=$@

output/medium.feather: src/generate.R
	Rscript --vanilla $< \
			--nrow=10000 \
			--ncol=15    \
			--npair=5200 \
			--output=$@

output/large.feather: src/generate.R
	Rscript --vanilla $< \
			--nrow=100000 \
			--ncol=20    \
			--npair=49315 \
			--output=$@

# done.
