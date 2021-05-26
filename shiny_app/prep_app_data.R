# =============================== #
# Prep Corpora Explorer Shiny App #
# =============================== #

library(corporaexplorer)

# Utilities
source('topicmodeling/processing_utils.R')

# ============= #
# Load database #
# ============= #

manifestos_db <- connect_to_db()
docs <- load_documents_table(conn = manifestos_db)
RMariaDB::dbDisconnect(manifestos_db)
rm(manifestos_db)


# ============================ #
# Prepare for corpora explorer #
# ============================ #

docs <- preprocess_docs(documents_table = docs)
docs_explore <- docs %>% 
  rename(Text = docs, Date = date, Title = title)
crp_explore <- prepare_data(
  dataset = docs_explore,
  corpus_name = 'Rebel Manifesto',
  date_based_corpus = TRUE,
  columns_doc_info = c('Title', 'country', 'group_name', 'doc_type', 'Date', 'Text'),
  columns_for_ui_checkboxes = c('group_name', 'doc_type')
)
saveRDS(crp_explore, file = 'shiny_app/corpora_explorer_object.rds', compress = F)