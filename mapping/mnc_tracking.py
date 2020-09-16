# import os
import sys

from pathlib import Path
import pandas as pd
import numpy as np

from data_input import file_loader as load

from config import registry as reg
from config import col_ids as col

# <editor-fold desc="#1 - Group soeur_rnd by MNC">

"""
Nota Bene:
- The soeur_mnc to newapp_mnc mapping id a one-to-many relation: step order is important. 
"""

year_index = [y for y in range(2003, 2020)]

# Load MNC consolidated RnD and exposure data file
# soeur_group_rnd = load.mnc_soeur_rnd(reg.project_path, r'data_input/soeur_rnd/MNCs_R&D_Exposure_v2_20200408.xlsx')
#
# soeur_group_rnd.drop(columns=['soeur_group_id', 'soeur_group_country_2DID', 'soeur_group_exposure_from_group_uc',
#                               'soeur_group_exposure_from_sector_uc', 'icb_3_name'],
#                      inplace=True)

soeur_group_rnd = pd.read_csv(
    reg.project_path.joinpath(r'ref_tables/SOEUR_RnD/SOEUR_RnD_2019b/SOEUR_RnD_2019b_20200309_by_group.csv'),
    na_values='#N/A',
    dtype={'orbis_parent_bvd9': str}
)

print('soeur_group_rnd_from_sector_uc: ' + str(soeur_group_rnd.rnd_clean.sum()))

# Reindex so that every group has values over the whole year range
soeur_group_rnd.set_index(keys=['soeur_group_name', 'year'], inplace=True)

new_index = pd.MultiIndex.from_product([
    soeur_group_rnd.index.levels[0].sort_values(),
    pd.Index(year_index).sort_values()
], names=['soeur_group_name', 'year'])

soeur_group_rnd = soeur_group_rnd.reindex(new_index)

soeur_group_rnd.reset_index(inplace=True)

# TODO: Check if grouping is needed (i.e. if the MNC mapping is more recent than the one used in soeur reference table
# Group at MNC level

# Load MNC reference table and map soeur_group_ids to parent_ids
mapping = pd.read_csv(
    reg.project_path.joinpath(r'ref_tables/soeur_to_orbis_parent.csv'),
    na_values='#N/A',
    dtype=str
)

soeur_group_rnd = pd.merge(
    soeur_group_rnd,
    mapping[['soeur_name', 'duplicate', 'rename']],
    left_on='soeur_group_name', right_on='soeur_name',
    how='left',
    suffixes=(False, False)
)

soeur_group_rnd['soeur_group_name'] = np.where(soeur_group_rnd['duplicate'] == 'FALSE',
                                               soeur_group_rnd['soeur_name'],
                                               soeur_group_rnd['rename'])

soeur_mnc_grouped = soeur_group_rnd.groupby(['soeur_group_name', 'year']).sum()

soeur_mnc_grouped.reset_index(inplace=True)

print('total_rnd read')

soeur_group_rnd = pd.read_csv(
    reg.project_path.joinpath(r'ref_tables/JRC004_MNC_soeur_group_rnd_20200825.csv'),
    na_values='#N/A'
)

soeur_mnc_grouped = pd.merge(
    soeur_mnc_grouped,
    soeur_group_rnd[['soeur_group_name', 'year', 'soeur_group_rnd']],
    left_on=['soeur_group_name', 'year'], right_on=['soeur_group_name', 'year'],
    how='left',
    suffixes=(False, False)
)

soeur_mnc_grouped.reset_index(inplace=True)

# Select values that are a current match
soeur_mnc_grouped = pd.merge(
    soeur_mnc_grouped,
    mapping[['soeur_name', 'orbis_parent_bvd_name', 'orbis_parent_bvd9', 'orbis_parent_country_2DID_iso']],
    left_on='soeur_group_name', right_on='soeur_name',
    how='left',
    suffixes=(False, False)
)

ref_country = pd.read_csv(reg.project_path.joinpath('ref_tables', 'country_table.csv'))

soeur_mnc_grouped = pd.merge(
    soeur_mnc_grouped,
    ref_country[['country_2DID_iso', 'world_player']],
    left_on='orbis_parent_country_2DID_iso', right_on='country_2DID_iso',
    how='left',
    suffixes=(False, False)
)

soeur_mnc_grouped.rename(columns={
    'orbis_parent_bvd9': 'MNC_bvd9',
    'orbis_parent_bvd_name': 'MNC_bvd_name',
    'rnd_group_UC': 'soeur_group_rnd_from_group_uc',
    'rnd_clean': 'soeur_group_rnd_from_sector_uc'
}, inplace=True)

soeur_mnc_grouped.reset_index(inplace=True)

# soeur_mnc_grouped.set_index(['MNC_bvd9', 'year'], inplace=True)

print('icb read')

icb = pd.read_csv(
    reg.project_path.joinpath(r'ref_tables/JRC004_MNC_icb_20200825.csv'),
    na_values='#N/A',
    dtype=str
)

print('icb merge')

soeur_mnc_grouped = pd.merge(
    soeur_mnc_grouped,
    icb[['Group_Name', 'ICB_3_digit_name']],
    left_on='soeur_group_name', right_on='Group_Name',
    how='left',
    suffixes=(False, False)
).rename(columns={'ICB_3_digit_name': 'icb_3_name'})



# Re-calculate exposure at MNC level
soeur_mnc_grouped['soeur_group_exposure_from_group_uc'] = soeur_mnc_grouped['soeur_group_rnd_from_group_uc'] / \
                                                          soeur_mnc_grouped['soeur_group_rnd']

soeur_mnc_grouped['soeur_group_exposure_from_sector_uc'] = soeur_mnc_grouped['soeur_group_rnd_from_sector_uc'] / \
                                                           soeur_mnc_grouped['soeur_group_rnd']

soeur_mnc_grouped = soeur_mnc_grouped.replace([np.inf, -np.inf], np.nan)

print('soeur_group_rnd_from_sector_uc: ' + str(soeur_mnc_grouped.soeur_group_rnd_from_sector_uc.sum()))
# print(soeur_mnc_grouped.info())
# print(soeur_mnc_grouped.head())
# </editor-fold>

# <editor-fold desc="#2 - Load rnd_new_approach">

newapp_mnc_table = load.mnc_newapp_rnd(
    r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Data\2019a_ORBIS_LC+HCO50_AA\parents - rnd_estimates.csv',
    'keep_all')

parent_ids = pd.read_csv(
    reg.parent_id_path,
    na_values='#N/A',
    dtype=col.dtype
)

newapp_mnc_table = pd.merge(
    newapp_mnc_table,
    parent_ids[['bvd9', 'company_name', 'parent_conso']],
    left_on=['bvd9', 'parent_conso'], right_on=['bvd9', 'parent_conso'],
    how='left',
    suffixes=(False, False)
)

newapp_mnc_table.rename(columns={
    'bvd9': 'MNC_bvd9',
    'company_name': 'MNC_bvd_name'
}, inplace=True)

newapp_mnc_table.reset_index(inplace=True)

print('newapp_parent_rnd_clean: ' + str(newapp_mnc_table.parent_rnd_clean.sum()))
# print(newapp_mnc_table.info())
# print(newapp_mnc_table.head())
# </editor-fold>

# <editor-fold desc="#3 - Consolidate MNC output table">

print('output merge')

output = pd.merge(
    soeur_mnc_grouped,
    newapp_mnc_table,
    left_on=['MNC_bvd9', 'year'], right_on=['MNC_bvd9', 'year'],
    how='outer',
    suffixes=('_soeur', '_newapp')
)

output['MNC_bvd_name'] = np.where(output['MNC_bvd_name_soeur'].isna(), output['MNC_bvd_name_newapp'],
                                  output['MNC_bvd_name_soeur'])

print('output rename')

output.rename(columns={
    'parent_oprev': 'orbis_parent_oprev',
    'parent_rnd': 'orbis_parent_rnd',
    'parent_exposure': 'newapp_parent_exposure',
    'parent_rnd_clean': 'newapp_parent_rnd_clean'
}, inplace=True)

print('output reset')

output.drop_duplicates(inplace=True)

output.reset_index(inplace=True)

print('soeur_group_rnd_from_sector_uc: ' + str(output.soeur_group_rnd_from_sector_uc.sum()))
print('newapp_parent_rnd_clean: ' + str(output.newapp_parent_rnd_clean.sum()))

# print(output.head())

output_cols = [
    'soeur_group_name',
    'MNC_bvd9',
    'MNC_bvd_name',
    # 'is_top_100',
    'world_player',
    'icb_3_name',
    'year',
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
    reg.case_path.joinpath(r'mnc-tracking.csv'),
    columns=output_cols,
    index=False,
    float_format='%.10f',
    na_rep='#N/A'
)
# </editor-fold>
