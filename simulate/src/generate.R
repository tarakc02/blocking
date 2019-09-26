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

library(argparse)

parser <- ArgumentParser()
parser$add_argument("--n_orig", type = "integer")
parser$add_argument("--n_pair", type = "integer")
parser$add_argument("--n_cols", type = "integer")
parser$add_argument("--n_col_derived", type = "integer")
parser$add_argument("--seed", type = "integer")
parser$add_argument("--output")
args <- parser$parse_args()

gen_records <- function(n_orig, n_cols, n_col_derived, n_pair) {
    alphabet_sizes <- sample(2:5, size = n_cols, replace = TRUE)
    word_lengths <- sample(3:6, size = n_cols, replace = TRUE)
    #     alphabet_sizes <- rpois(lambda = 4, n = n_cols)
    #     word_lengths <- rpois(lambda = 3, n = n_cols)

    main <- map2(word_lengths, alphabet_sizes, gen_col, n = n_orig) %>%
        bind_cols %>% set_names(paste0("r", seq_len(n_cols)))

    additions <- main[, which(word_lengths > 3)] %>%
        mutate_all(derive_feature, from = 1, to = 3) %>%
        set_names(str_replace, "^r", "da")

    additions2 <- main[, which(word_lengths > 3)] %>%
        mutate_all(derive_feature, from = 2, to = 4) %>%
        set_names(str_replace, "^r", "db")

    additions3 <- main[, which(word_lengths > 4)] %>%
        mutate_all(derive_feature, from = 3, to = 5) %>%
        set_names(str_replace, "^r", "dc")

    additions4 <- main[, which(word_lengths > 5)] %>%
        mutate_all(derive_feature, from = 4, to = 6) %>%
        set_names(str_replace, "^r", "dd")

    uniq <- bind_cols(id = seq_len(n_orig), main,
                      additions, additions2, additions3, additions4)

    derived_colnames <- colnames(uniq)[str_which(colnames(uniq), "^d")]
    if (length(derived_colnames) < n_col_derived) stop("not enough derived cols")

    keep_derived <- sample(derived_colnames,
                           size = n_col_derived,
                           replace = FALSE)
    uniq %>% select(id, starts_with("r"), keep_derived)
    dupes <- gen_pairs(uniq, n_pair)
    bind_rows(uniq, dupes)
}

derive_feature <- function(column, from, to) {
    str_sub(column, from, to)
}

gen_pairs <- function(records, np) {
    dupes <- sample_n(records, np, replace = TRUE)
    dupes <- mutate_at(dupes, vars(starts_with("r")), add_noise,
                       noise_prob = .2)
    dupes <- mutate_at(dupes, vars(starts_with("d")), add_noise,
                       noise_prob = .1)
    dupes
}

gen_col <- function(len, alph_size, n) {
    replicate(n = n, sample(letters[seq_len(alph_size)],
                            replace = TRUE,
                            size = len), simplify = FALSE) %>%
        map_chr(paste, collapse = "")
}

add_noise <- function(column, noise_prob) {
    has_noise <- rbernoulli(length(column), noise_prob)
    noise <- sample(column, size = sum(has_noise), replace = TRUE)
    column[has_noise] <- noise
    column
}

set.seed(args$seed)
output <- gen_records(args$n_orig, args$n_cols,
                      args$n_col_derived, args$n_pair)
output <- mutate(output, recordid = seq_len(nrow(output)))
write_feather(output, args$output)

# done.

