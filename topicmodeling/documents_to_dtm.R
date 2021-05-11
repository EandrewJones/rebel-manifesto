# ==================== #
# Process Text for NLP #
# ==================== #

# Utilities
source('topicmodeling/processing_utils.R')

# ============= #
# Load database #
# ============= #

manifestos_db <- connect_to_db()
docs <- load_documents_table(conn = manifestos_db)
RMariaDB::dbDisconnect(manifestos_db)
rm(manifestos_db)


# ========================================== #
# Convert to corpora, tokenize, and make dfm #
# ========================================== #

docs <- preprocess_docs(documents_table = docs)
docs_corpus <- create_corpus(documents_table = docs)
tokens <- create_tokens(corp = docs_corpus)
dtm <- create_dfm(
  toks = tokens,
  stem = F,
  max_termfreq = 0.95,
  termfreq_type = "quantile"
  )
stm_data <- convert_to_stm(dtm = dtm, corp = docs_corpus, toks = tokens)


# ==== #
# Save #
# ==== #

obj_to_save <- list(docs, docs_corpus, tokens, dtm, stm_data)
fname_to_save <- list("docs", "corpus", "tokens", "dtm", "stm_df")
walk2(
  obj_to_save,
  fname_to_save,
  ~ saveRDS(object = .x, file = paste0("data/", .y, ".RDS"))
)
