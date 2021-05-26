# ========================== #
# Corpora Explorer Shiny App #
# ========================== #

library(corporaexplorer)
corpus <- readRDS('corpora_explorer_object.rds')
explore(corpus)
