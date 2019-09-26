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
			--n_orig=1000 \
			--n_pair=612 \
			--n_cols=3 \
			--n_col_derived=6 \
			--seed=19730781 \
			--output=$@

output/medium.feather: src/generate.R
	Rscript --vanilla $< \
			--n_orig=10000 \
			--n_pair=5200 \
			--n_cols=6 \
			--n_col_derived=13 \
			--seed=37020739 \
			--output=$@

output/large.feather: src/generate.R
	Rscript --vanilla $< \
			--n_orig=100000 \
			--n_pair=47207 \
			--n_cols=9 \
			--n_col_derived=16 \
			--seed=1089672372 \
			--output=$@

# done.
