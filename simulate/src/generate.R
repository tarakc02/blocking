#
# Authors:     TS
# Maintainers: TS
# Copyright:   2019, HRDAG, GPL v2 or later
# =========================================
# blocking/simulate/src/generate.R

library(dplyr)
library(purrr)
library(stringr)
library(feather)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--nrow", default = 1000, type = "integer")
parser$add_argument("--ncol", default = 10, type = "integer")
parser$add_argument("--npair", default = 472, type = "integer")
parser$add_argument("--output")
args <- parser$parse_args()

n_rows <- args$nrow
n_cols <- args$ncol
n_pairs <- args$npair

gen_records <- function(n_rows, n_cols, n_pairs) {
    alphabet_sizes <- rpois(lambda = 4, n = n_cols)
    word_lengths <- rpois(lambda = 4, n = n_cols)

    main <- map2(word_lengths, alphabet_sizes, gen_col, n = n_rows) %>%
        bind_cols %>% set_names(paste0("r", seq_len(n_cols)))

    additions <- main[, which(word_lengths > 4)] %>%
        mutate_all(derive_feature) %>%
        set_names(str_replace, "^r", "d")

    main <- bind_cols(id = seq_len(n_rows), main, additions)
    duplicates <- gen_pairs(main, np = n_pairs)
    bind_rows(main, duplicates)
}

gen_pairs <- function(records, np) {
    dupes <- sample_n(records, np)
    dupes <- mutate_at(dupes, vars(-id), add_noise)
    dupes
}

gen_col <- function(len, alph_size, n) {
    replicate(n = n, sample(letters[seq_len(alph_size)],
                            replace = TRUE,
                            size = len), simplify = FALSE) %>%
        map_chr(paste, collapse = "")
}

derive_feature <- function(column) {
    str_sub(column, 1, 3)
}

add_noise <- function(column, noise_prob = .25) {
    has_noise <- rbernoulli(length(column), noise_prob)
    noise <- sample(column, size = sum(has_noise), replace = TRUE)
    column[has_noise] <- noise
    column
}

output <- gen_records(n_rows, n_cols, n_pairs)
output <- mutate(output, recordid = seq_len(nrow(output)))
write_feather(output, args$output)

# done.

