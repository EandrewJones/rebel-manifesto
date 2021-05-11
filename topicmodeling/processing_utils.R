# ================================= #
# Text Processing Utility functions #
# ================================= #


# libraries
library(quanteda)
library(RMariaDB)
library(tidyverse)
library(stringr)
library(purrr)

# Initialize spacy
source('topicmodeling/rebel_manifesto_init.R')

# ========= #
# Functions #
# ========= #


# Not in
`%nin%` <- Negate(`%in%`)


connect_to_db <- function() {
  # Function for connecting to manifestos_db database
  #
  # Returns
  # -------
  # Returns RMariaDB database connection
  group <- "r_mysql_db"
  rmariadb_settingsfile <- list.files(".env", pattern = ".cnf", full.names = T)
  manifestos_db <- dbConnect(
    RMariaDB::MariaDB(),
    default.file = rmariadb_settingsfile,
    group = group
  )
  return(manifestos_db)
}


load_documents_table <- function(conn) {
  # Function to load documents from sql database
  #
  # Parameters
  # ----------
  # Conn: connection to manifestos_db database
  #
  # Returns
  # -------
  # tibble
  
  
  # Select all data
  query <- "SELECT * FROM documents;"
  results <- dbSendQuery(conn=conn, statement=query)
  documents <- dbFetch(results)
  dbClearResult(results)
  
  # Filter untranslated documents
  # create single document column
  docs_filter <- documents %>% 
    filter(!(language != 'en' & is_translated == 0)) %>% 
    mutate(docs = ifelse(language == 'en', orig_text, trans_text),
           id = paste(group_name, id, sep = '_')) %>% 
    select(id:date, docs)
  return(docs_filter)
}


count_days <- function(d) {
  # Function for converting date object into a count of number of days
  # since starting date
  #
  # Parameters
  # ----------
  # d: datetime object
  #
  # Returns
  # -------
  # day_count: integer
  startdate <- range(d)[1]
  day_count <- difftime(d, startdate, units = "days") %>% as.integer()
  return(day_count)
}


preprocess_docs <- function(documents_table) {
  # Function to clean up text prior to spacy ingestion
  #
  # Parameters
  # ----------
  # documents_table: data.frame or tbl
  #   The documents table from manifestos_db
  #
  # Returns
  # -------
  # qdocuments_table: data.frame or tbl
  documents_table %>% 
    mutate(
      title = title %>% str_trim(),
      group_name = as.factor(group_name),
      country = as.factor(country),
      doc_type = doc_type %>% 
        tolower() %>% 
        ifelse(grepl('news', .), 'news', .) %>% # TODO: make more generalizeable
        as.factor(), 
      days = count_days(date),
      docs = docs %>% 
        # Remove untranslated phrases
        iconv(., "latin1", "ASCII", sub="") %>% 
        # trim ws
        str_trim() %>% 
        str_replace_all('\\s+', " ") %>% 
        # Remove text inside parentheses
        str_remove_all('\\s*\\([^\\)]+\\)')
      )
}

create_corpus <- function(documents_table) {
  # Function to convert documents into a quanteda corpus
  #
  # Parameters
  # ----------
  # documents_table: data.frame or tbl
  #   The documents table from manifestos_db
  #
  # Returns
  # -------
  # quanteda corpus
  txt_corpus <- quanteda::corpus(
    documents_table$docs,
    docnames = docs$id, 
    docvars = docs %>% select(id:date, days)
  )
  return(txt_corpus)
}


create_tokens <- function(corp) {
  # Function to tokenize quanteda corpus
  #
  # Parameters
  # ----------
  # corp: quanteda corpus
  #
  # Returns
  # -------
  # tokens
  sw <- quanteda::stopwords()
  toks <- corp %>% 
    # Parse with pos, entity and nounphrase tagging
    spacy_parse(.,
                lemma = FALSE,
                entity = TRUE,
                nounphrase = TRUE,
                multithread = FALSE
    ) %>%
    dplyr::filter(pos %nin% c('PUNCT', 'SPACE', 'SYM')) %>% 
    # consolidate entities
    entity_consolidate() %>%
    # remove stopwords, numbers, punctuation, spaces, determiners, and symbols
    dplyr::mutate(stopword = (token %in% sw)) %>%
    dplyr::filter(!stopword & (pos %nin% c("NUM", "DET"))) %>%
    # clean up tokens and doc ids
    dplyr::mutate(
      token = token %>% tolower()
      ) %>% 
    dplyr::select(-stopword) %>%
    # convert to tokens
    as.tokens()
  return(toks)
}



create_dfm <- function(toks, stem = TRUE, ...) {
  # Function to convert corpus to document-feature matrix
  #
  # Parameters
  # ----------
  # toks: tokens,
  #   tokenized text
  # stem: logical
  #   whether to stem tokens.
  # ...: 
  #   additional arguments to be passed to dfm_trim(), See ?dfm_trim for more
  #   details.
  #
  # Returns
  # -------
  # quanteda document-feature matrix
  dtm <- toks %>% 
    dfm()
  if (stem) dtm <- dtm %>% dfm_wordstem()
  dtm <- dtm %>% 
    dfm_trim(...)
  return(dtm)
}


convert_to_stm <- function(dtm, corp, toks) {
  # Function for converting a dtm to an stm-friendly object
  # 
  # Parameters
  # ----------
  # dtm: a quanteda document-feature matrix (dfm)
  #   The object to be converted.
  # corp: a quaenteda corpus
  #   The corpus from which the tokens in the dfm were created. 
  #   Must include document-level variables
  # toks: quanteda tokens
  #   The tokens used to created the dfm.
  #
  # Returns
  # -------
  # An stm ready object
  
  # Get docvars
  docs_in_tokens <- toks %>% docnames()
  subset_corp <- corpus_subset(corp, id %in% docs_in_tokens)
  corp_vars <- docvars(subset_corp) %>% arrange(id)
  
  # convert dtm to stm
  stm_obj <- convert(dtm, to = 'stm', docvars = corp_vars)
  return(stm_obj)
}
