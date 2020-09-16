# Import libraries

import pandas as pd
import datetime
import json

from rnd_new_approach import rnd_methods as mtd

from config import registry as reg
from config import col_ids as col

# TODO: transfer in a specific report.py file and transfer report.py in method.py
# TODO: How does disclosed rnd and oprev in subs compare with rnd and oprev in parents
# TODO: How much disclosed sub rnd is embedded in keyword matching subs and not accounted for in subs final rnd
# TODO: How much disclosed parent rnd is embedded in parent that have a potential keyword match but have no subsidiaries
# TODO: Select top 10 parents in each cluster and consolidate oprev, exposure and rnd trends over range_ys
# TODO: Distribution and cumulative distribution functions for parent and subs (rnd x oprev or market cap?) by world_player
# TODO: Ex-post exposure global and by world_player

rnd_conso = pd.DataFrame()

rnd_conso_cols = ['vintage', 'approach', 'method', 'year', 'rnd_clean', 'sub_world_player',
                  'sub_country_2DID_iso']

print('> Prepare soeur_rnd ...')

ref_soeur_path = r'H:\PycharmProjects\rnd-private\ref_tables\SOEUR_RnD\SOEUR_RnD_2019b\SOEUR_RnD_2019b_20200309_by_region_n_tech.csv'

soeur_rnd_grouped = pd.read_csv(
    r'H:\PycharmProjects\rnd-private\ref_tables\SOEUR_RnD\SOEUR_RnD_2019b\SOEUR_RnD_2019b_20200309_by_region_n_tech.csv',
    na_values='#N/A')

soeur_rnd_grouped['approach'] = 'SOEUR_rnd_2019b_20200309'

soeur_rnd_grouped['method'] = 'keep_all'

soeur_rnd_grouped['vintage'] = soeur_rnd_grouped['approach'] + ' - ' + soeur_rnd_grouped['method']

# print('> Prepare mnc_rnd ...')
#
# mnc_rnd_grouped = mtd.load_n_group_MNC_rnd()
#
# mnc_rnd_grouped.rename(columns={
#     'group_country_3DID_iso': 'country_3DID_iso',
#     'group_world_player': 'world_player',
#     'group_rnd_clean': 'rnd_clean'
# }, inplace=True)

print('> Consolidated dataframe ...')

soeur_rnd_grouped.rename(columns={
    'soeur_sub_country_2DID_iso': 'sub_country_2DID_iso',
    'soeur_sub_world_player': 'sub_world_player'
}, inplace=True)

rnd_conso = rnd_conso.append(soeur_rnd_grouped[rnd_conso_cols])

print('soeur_rnd: ' + str(rnd_conso.rnd_clean.sum()))

newapp_sub_rnd_grouped = pd.read_csv(
    r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Data\2019a_ORBIS_LC+HCO50_AA\subsidiaries - rnd_estimates.csv',
    na_values='#N/A',
    dtype=col.dtype)

newapp_sub_rnd_grouped = newapp_sub_rnd_grouped.groupby(['year', 'sub_country_2DID_iso', 'sub_world_player', 'method']).sum()

newapp_sub_rnd_grouped.rename(columns={
    'sub_rnd_clean': 'rnd_clean'
}, inplace=True)

newapp_sub_rnd_grouped.reset_index(inplace=True)

newapp_sub_rnd_grouped['approach'] = 'newapp_ORBIS_2019a_20200826'

newapp_sub_rnd_grouped['vintage'] = newapp_sub_rnd_grouped['approach'] + ' - ' + newapp_sub_rnd_grouped['method']

rnd_conso = rnd_conso.append(newapp_sub_rnd_grouped[rnd_conso_cols])

print('newapp_rnd: ' + str(newapp_sub_rnd_grouped.rnd_clean.sum()))

print('> Save output table ...')

rnd_conso.to_csv(r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Data\2019a_ORBIS_LC+HCO50_AA\benchmark.csv',
                 columns=rnd_conso_cols,
                 float_format='%.10f',
                 index=False,
                 na_rep='#N/A'
                 )