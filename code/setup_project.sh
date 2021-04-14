#
# Updates manifesto directory based on current files
#
#! /bin/bash


cd ~/Documents/projects/rebel_manifesto

# Setup environment and update dictionary
/opt/conda/envs/python/bin/python code/create_dictionary.py > /dev/null

echo "Rebel manifesto dictionary updated."
exit 1
