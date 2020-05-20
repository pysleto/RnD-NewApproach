# import os
import sys

from pathlib import Path
import pandas as pd
import numpy as np

from data_input import file_loader as load

#TODO: Implemenent an integrated config and init so that all changes are centralized and distributed in all scripts in an updated/common fashion

# Set  data frame display options
pd.options.display.max_columns = None
pd.options.display.width = None

root = Path(r'C:\Users\Simon\PycharmProjects\rnd-private')

rnd_ys = [YYYY for YYYY in range(2003, 2019)]

# <editor-fold desc="#1 - Group soeur_rnd by MNC">

"""
Nota Bene:
- The soeur_mnc to newapp_mnc mapping id a one-to-many relation: step order is important. 
"""

# TODO: Consolidate directly from the soeur output file
# Load MNC consolidated RnD and exposure data file
soeur_group_rnd = load.mnc_soeur_rnd(root, r'data_input/soeur_rnd/MNCs_R&D_Exposure_v2_20200408.xlsx')

soeur_group_rnd.drop(columns=['soeur_group_id', 'soeur_group_country_2DID', 'icb_3_name',
                              'soeur_group_exposure_from_group_uc', 'soeur_group_exposure_from_sector_uc'],
                     inplace=True)

# Reindex so that every group has values over the whole year range
soeur_group_rnd.set_index(keys=['soeur_group_name', 'year'], inplace=True)

new_index = pd.MultiIndex.from_product([
    soeur_group_rnd.index.levels[0].sort_values(),
    pd.Index(rnd_ys).sort_values()
], names=['soeur_group_name', 'year'])

soeur_group_rnd = soeur_group_rnd.reindex(new_index)

soeur_group_rnd.reset_index(inplace=True)

# Load MNC reference table and map soeur_group_ids to parent_ids
mapping = pd.read_csv(
    root.joinpath(r'ref_tables/MNC_jrc004_to_newapp_20200420.csv'),
    na_values='#N/A',
    dtype=str
)

# Select values that are a current match
soeur_group_rnd = pd.merge(
    soeur_group_rnd,
    mapping,
    left_on='soeur_group_name', right_on='soeur_group_name',
    how='inner',
    suffixes=(False, False)
)  # .drop(columns=['soeur_group_name'])

# TODO: Check if grouping is needed (i.e. if the MNC mapping is more recent than the one used in soeur reference table
# Group at MNC level
soeur_mnc_grouped = soeur_group_rnd.groupby(['MNC_bvd9', 'MNC_bvd_name', 'year', 'world_player', 'ICB_id']).sum()

soeur_mnc_grouped.reset_index(level=['MNC_bvd_name', 'world_player', 'ICB_id'], inplace=True)

# Re-calculate exposure at MNC level
soeur_mnc_grouped['soeur_group_exposure_from_group_uc'] = soeur_mnc_grouped['soeur_group_rnd_from_group_uc'] / \
                                                          soeur_mnc_grouped['soeur_group_rnd']

soeur_mnc_grouped['soeur_group_exposure_from_sector_uc'] = soeur_mnc_grouped['soeur_group_rnd_from_sector_uc'] / \
                                                           soeur_mnc_grouped['soeur_group_rnd']

soeur_mnc_grouped = soeur_mnc_grouped.replace([np.inf, -np.inf], np.nan)

# print('soeur_group_rnd_from_sector_uc: ' + str(soeur_mnc_grouped.soeur_group_rnd_from_sector_uc.sum()))
# print(soeur_mnc_grouped.head())
# </editor-fold>

# <editor-fold desc="#2 - Load rnd_new_approach">

# TODO: load from new approach reference table
newapp_mnc_table = load.mnc_newapp_rnd(root, r'cases/2018_GLOBAL/6 - rnd_estimates - parents.csv', 'keep_all')

newapp_mnc_table.rename(columns={'bvd9': 'MNC_bvd9'}, inplace=True)

newapp_mnc_table.set_index(['MNC_bvd9', 'year'], inplace=True)

newapp_mnc_table.sort_index(inplace=True)

# print(newapp_mnc_table.head())
# </editor-fold>

# <editor-fold desc="#3 - Consolidate MNC output table">

output = pd.merge(
    soeur_mnc_grouped,
    newapp_mnc_table,
    left_index=True, right_index=True,
    how='left',
    suffixes=(False, False)
)

# Load ICB reference table and update
icb = pd.read_csv(
    root.joinpath(r'ref_tables/Industry_classification_ICB_20200419.csv'),
    index_col='ICB_id',
    na_values='#N/A',
    dtype=str
)

output = pd.merge(
    output,
    icb['ICB_3_name'],
    left_on='ICB_id', right_index=True,
    how='left',
    suffixes=(False, False)
)

# Identify top 100 investors over all period
top_100 = output[['soeur_group_rnd_from_sector_uc', 'newapp_parent_rnd_clean']].reset_index(level=1, drop=True).sum(
    axis=1)

top_100 = top_100.groupby('MNC_bvd9').sum().nlargest(100).reset_index()['MNC_bvd9']

# print(top_100)

output.reset_index(inplace=True)

output['is_top_100'] = output.MNC_bvd9.isin(top_100)

# print(output.head())

output_cols = [
    'MNC_bvd9',
    'year',
    'MNC_bvd_name',
    'world_player',
    'ICB_3_name',
    'is_top_100',
    'soeur_group_rnd',
    'soeur_group_rnd_from_group_uc',
    'soeur_group_rnd_from_sector_uc',
    'soeur_group_exposure_from_group_uc',
    'soeur_group_exposure_from_sector_uc',
    'orbis_parent_oprev',
    'orbis_parent_rnd',
    'newapp_parent_exposure',
    'newapp_parent_rnd_clean']

output.to_csv(
    root.joinpath(r'mnc_tracking\mnc-tracking.csv'),
    columns=output_cols,
    index=False,
    float_format='%.10f',
    na_rep='#N/A'
)
# </editor-fold>
