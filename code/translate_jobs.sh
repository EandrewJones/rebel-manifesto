#! /bin/bash

# setup environment
cd /home/evan/Documents/projects/rebel_manifesto/code
source /opt/conda/etc/profile.d/conda.sh
conda activate /home/evan/.conda/envs/rebel_manifesto

# Run job
# TODO: add arg parser to pass args thru
nohup python -u translate_jobs.py \
    -s /home/evan/Documents/projects/rebel_manifesto/manifestos/Canada/PQ/primary \
    -j /home/evan/Documents/projects/rebel_manifesto/data/pq_jobs.pkl \
    -b 400000 \
    >> ../logs/translate_pq.log &

exit 1


