#! /bin/bash

# setup environment
cd /home/evan/Documents/projects/rebel_manifesto

# Run jobs
# TODO: add logging capabilitites to each script
nohup Rscript topicmodeling/documents_to_dtm.R > logs/documents_to_dtm.log &

exit 1