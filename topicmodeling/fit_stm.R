# ======================= #
# Structural Topic Models #
# ======================= #


# libraries
library(stm)
library(tidyverse)

# Load data
stm_df <- readRDS('data/stm_df.RDS')


# ======================= #
# Search Number of Topics #
# ======================= #


library(furrr)
plan(multiprocess)

many_models <- tibble(K = c(25, 30, 35, 40, 50, 60, 75, 80, 100, 120)) %>% 
  mutate(topic_model = future_map(K,
                                  ~ stm(documents = stm_df$documents,
                                        vocab = stm_df$vocab,
                                        K = .x,
                                        prevalence = ~ country + group_name + 
                                          doc_type + s(days),
                                        data = stm_df$meta,
                                        seed = TRUE,
                                        init.type = 'Spectral',
                                        max.em.its = 5000,
                                        verbose = FALSE,
                                        )))
heldout <- make.heldout(stm_df$documents, stm_df$vocab)
k_result <- many_models %>%
  mutate(
    exclusivity = map(topic_model, exclusivity),
    semantic_coherence = map(topic_model, semanticCoherence, stm_df$documents),
    eval_heldout = map(topic_model, eval.heldout, heldout$missing),
    residual = map(topic_model, checkResiduals, stm_df$documents),
    bound =  map_dbl(topic_model, function(x) max(x$convergence$bound)),
    lfact = map_dbl(topic_model, function(x) lfactorial(x$settings$dim$K)),
    lbound = bound + lfact,
    iterations = map_dbl(topic_model, function(x) length(x$convergence$bound))
    )
k_result


# ==== #
# Save #
# ==== #

obj_to_save <- list(many_models, k_result)
fname_to_save <- list("stm_fits", "stm_diagnostics")
walk2(
  obj_to_save,
  fname_to_save,
  ~ saveRDS(object = .x, file = paste0("data/", .y, ".RDS"))
)

