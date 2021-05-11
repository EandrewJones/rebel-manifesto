# ================ #
# Analyze stm fits #
# ================ #

# library
library(stm)
library(tidytext)
library(tidyverse)
library(ggthemes)

# Load data
docs <- readRDS('data/docs.RDS')
stm_diag <- readRDS('data/stm_diagnostics.RDS')
stm_df <- readRDS('data/stm_df.RDS')

# =============== #
# Identify models #
# =============== #

# Diagnostic plots
stm_diag %>%
  transmute(
    K,
    `Lower Bound` = lbound,
    Residuals = map_dbl(residual, "dispersion"),
    `Semantic coherence` = map_dbl(semantic_coherence, mean),
    `Held-out likelihood` = map_dbl(eval_heldout, "expected.heldout")
  ) %>%
  gather(Metric, Value, -K) %>%
  ggplot(aes(K, Value, color = Metric)) +
  geom_line(size = 1.5, alpha = 0.7, show.legend = FALSE) +
  facet_wrap(~Metric, scales = 'free_y') +
  labs(x = "K (number of topics)",
       y = NULL,
       title = "Model diagnostics by number of topics")
stm_diag %>%
  select(K, exclusivity, semantic_coherence) %>%
  filter(K %in% c(30, 40, 50, 75)) %>%
  unnest(cols = c(exclusivity, semantic_coherence)) %>%
  mutate(K = as.factor(K)) %>%
  ggplot(aes(semantic_coherence, exclusivity, color = K)) +
  geom_point(size = 2, alpha = 0.7) +
  labs(x = "Semantic coherence",
       y = "Exclusivity",
       title = "Comparing exclusivity and semantic coherence",
       subtitle = "Models with fewer topics have higher semantic coherence for more topics, but lower exclusivity")

# ====================== #
# Examine 60 topic model #
# ====================== #

topic_model <- stm_diag %>%
  filter(K == 40) %>%
  pull(topic_model) %>%
  .[[1]]
topic_model

# Examine Topics

td_beta <- tidy(topic_model)
td_gamma <- tidy(topic_model, matrix = "gamma", document_names = docs$title)

top_terms <- td_beta %>%
  arrange(beta) %>%
  group_by(topic) %>%
  top_n(7, beta) %>%
  arrange(-beta) %>%
  select(topic, term) %>%
  summarise(terms = list(term)) %>%
  mutate(terms = map(terms, paste, collapse = ", ")) %>%
  unnest(cols = c(terms))

gamma_terms <- td_gamma %>%
  group_by(topic) %>%
  summarise(gamma = mean(gamma)) %>%
  arrange(desc(gamma)) %>%
  left_join(top_terms, by = "topic") %>%
  mutate(topic = paste0("Topic ", topic),
         topic = reorder(topic, gamma))

# Plot top terms
gamma_terms %>%
  top_n(20, gamma) %>%
  ggplot(aes(topic, gamma, label = terms, fill = topic)) +
  geom_col(show.legend = FALSE) +
  geom_text(hjust = 0, nudge_y = 0.0005, size = 3,
            family = "IBMPlexSans") +
  coord_flip() +
  scale_y_continuous(expand = c(0,0),
                     limits = c(0, 0.09),
                     labels = scales::percent_format()) +
  theme_tufte(base_family = "IBMPlexSans", ticks = FALSE) +
  theme(plot.title = element_text(size = 16,
                                  family="IBMPlexSans-Bold"),
        plot.subtitle = element_text(size = 13)) +
  labs(x = NULL, y = expression(gamma),
       title = "Top 20 topics by prevalence in the Hacker News corpus",
       subtitle = "With the top words that contribute to each topic")

# Topics by prevalence
gamma_terms %>%
  select(topic, gamma, terms)

labelTopics(topic_model)

# Find thoughts for 12, 59
modeled_docs <- stm_df$documents %>% names()
modeled_docs_text <- docs %>% filter(id %in% modeled_docs) %>% .$docs
findThoughts(
  topic_model,
  texts = modeled_docs_text,
  n = 5,
  topics = 1
)$docs[[1]]


# =============================== #
# Estimate Efffects for HR topics #
# =============================== #

est_effects <- estimateEffect(
  1 ~ -1 + country + group_name + doc_type + s(days),
  topic_model,
  metadata = stm_df$meta,
  uncertainty = "Global"
)
summary(est_effects, topics = 1)
