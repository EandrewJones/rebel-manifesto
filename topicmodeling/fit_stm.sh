#! /bin/bash

# setup environment
cd /home/evan/Documents/projects/rebel_manifesto

# Run jobs
# TODO: add logging capabilitites to each script
nohup Rscript topicmodeling/fit_stm.R > logs/fit_stm.log &

exit 1