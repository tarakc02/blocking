#
# Authors:     TS
# Maintainers: TS
# Copyright:   2019, HRDAG, GPL v2 or later
# =========================================
# blocking/reshape/src/prep-data.R

library(dplyr)
library(tidyr)
library(purrr)
library(feather)
library(argparse)

parser <- ArgumentParser()
parser$add_argument("--input", default = "input/small.feather")
parser$add_argument("--output")
args <- parser$parse_args()

train <- read_feather(args$input)

record_ids <- select(train, recordid, id)

pairs <- record_ids %>%
    inner_join(record_ids, by = "id", suffix = c("_1", "_2")) %>%
    distinct(recordid_1, recordid_2) %>%
    filter(recordid_1 < recordid_2)

coverage <- function(...) {
    tb1 <- select(train, recordid, ...)
    tb2 <- select(train, recordid, ...)

    joined <- pairs %>%
        inner_join(tb1, by = c("recordid_1" = "recordid")) %>%
        inner_join(tb2, by = c("recordid_2" = "recordid"))

    nms <- colnames(tb1)[!colnames(tb1) %in% c("recordid", "id")]

    f <- function(nm) {
        coalesce(joined[[paste0(nm, ".x")]] == joined[[paste0(nm, ".y")]], FALSE)
    }

    bind_cols(pairs, as_tibble(map(nms, f) %>% set_names(nms)))
}

pairsout <- coverage(colnames(train)) %>%
    mutate(pairid = seq_len(nrow(.))) %>%
    select(pairid, everything())

write_feather(pairsout, args$output)

# done.

