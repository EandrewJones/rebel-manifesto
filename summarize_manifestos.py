import os
import pprint
import pandas as pd
import numpy as np

pp = pprint.PrettyPrinter()
os.chdir('manifestos')

# ================= #
# Utility functions #
# ================= #


def path_to_dict(path, d):
    name = os.path.basename(path)
    if os.path.isdir(path):
        if name not in d['dirs']:
            d['dirs'][name] = {'dirs': {}, 'files': []}
        for x in os.listdir(path):
            path_to_dict(os.path.join(path, x), d['dirs'][name])
    else:
        d['files'].append(name)
    return d

# ================================ #
# Create manifesto stat data frame #
# ================================ #

# Convert directory into dictionary
startpath = '.'
dct = path_to_dict(startpath, d = {'dirs': {}, 'files': []})
pp.pprint(dct['dirs']['.']['dirs'])

# Clean out 'dir' and 'files' keys
country_dct = dct['dirs']['.']['dirs']
country_dct = {i: {j: {k: [f 
                           for f in country_dct[i]['dirs'][j]['dirs'][k]['files']]
                       for k in country_dct[i]['dirs'][j]['dirs']} 
                   for j in country_dct[i]['dirs']} 
               for i in country_dct.keys()}
pp.pprint(country_dct)

# Convert to data frame
manifesto_df = pd.DataFrame.from_dict({(i, j): country_dct[i][j]
                                       for i in country_dct.keys()
                                       for j in country_dct[i].keys()}, 
                                      orient='index')
manifesto_df.reset_index(inplace=True)
manifesto_df.rename(columns={'level_0': 'country', 'level_1': 'group'},
                    inplace=True)
manifesto_df

# Convert to long-form
manifesto_df = pd.melt(manifesto_df, 
                       id_vars=['country', 'group'],
                       value_vars=['primary', 'secondary'], 
                       var_name='source_type',
                       value_name='file_name')
manifesto_df = pd.DataFrame({
    col: np.repeat(manifesto_df[col].values, manifesto_df['file_name'].str.len())
    for col in manifesto_df.columns.drop('file_name')
    }).assign(**{'file_name': np.concatenate(manifesto_df['file_name'].values)})
manifesto_df.sort_values(['country', 'group'], inplace=True)

# Add file type
manifesto_df['file_type'] = manifesto_df.file_name.str.rsplit('.', 1, expand = True)[1]

# Add date
manifesto_df['date'] = manifesto_df.file_name.str.extract(r'(\d{4}(-\d+-\d+)?)')[0]

# Add language
manifesto_df['language'] = manifesto_df.file_name.str.extract(r'(\[(\w+?)\])')[1]
manifesto_df['language'].fillna('EN', inplace=True)

# Add description and document_type to primary sources
mdf_primary = manifesto_df[manifesto_df['source_type'] == 'primary']
splits = mdf_primary.file_name.str.rsplit('.', 1, expand=True)[0].str.split('_', expand=True)
mdf_primary['description'] = splits[0]
mdf_primary['document_type'] = splits[1]

# Add author, description, and document type to secondary sources
# TODO fix file names 
mdf_secondary = manifesto_df[manifesto_df['source_type'] == 'secondary']
splits = mdf_secondary.file_name.str.rsplit('.', 1, expand=True)[0].str.split('_', expand=True)