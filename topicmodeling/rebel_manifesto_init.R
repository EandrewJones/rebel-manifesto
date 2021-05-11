# ============ #
# Python setup #
# ============ #

# dependencies
library(reticulate)
library(spacyr)

# Activate conda environment
# spacyr::spacy_download_langmodel(model = "en", envname = 'rebel_manifesto')
spacyr::spacy_initialize(condaenv = 'rebel_manifesto')
