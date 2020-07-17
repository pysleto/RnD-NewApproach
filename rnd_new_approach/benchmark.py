# Import libraries

import pandas as pd
import datetime
import json

from rnd_new_approach import rnd_methods as mtd

from config import registry as reg

# TODO: transfer in a specific report.py file and transfer report.py in method.py
# TODO: How does disclosed rnd and oprev in subs compare with rnd and oprev in parents
# TODO: How much disclosed sub rnd is embedded in keyword matching subs and not accounted for in subs final rnd
# TODO: How much disclosed parent rnd is embedded in parent that have a potential keyword match but have no subsidiaries
# TODO: Select top 10 parents in each cluster and consolidate oprev, exposure and rnd trends over range_ys
# TODO: Distribution and cumulative distribution functions for parent and subs (rnd x oprev or market cap?) by world_player
# TODO: Ex-post exposure global and by world_player

rnd_conso_cols = ['vintage', 'approach', 'method', 'year', 'sub_rnd_clean', 'guo_type', 'type', 'sub_world_player',
                  'sub_country_3DID_iso', 'cluster', 'technology', 'priority', 'action']

print('> Prepare soeur_rnd ...')

ref_soeur_path = r'C:\Users\Simon\PycharmProjects\rnd-private\ref_tables/SOEUR_RnD/SOEUR_RnD_2019b_20200309.csv'

(soeur_rnd_grouped, embedded_soeur_rnd_grouped) = mtd.group_soeur_rnd_for_bench(ref_soeur_path)

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



# TODO : Integrate embedded in MNC rnd
rnd_conso = rnd_conso.append(soeur_rnd_grouped)

rnd_conso = rnd_conso.append(sub_rnd_grouped)

# rnd_conso = rnd_conso.append(mnc_rnd_grouped)

print('> Re-group for tailored output table ...')

rnd_conso = rnd_conso.groupby(['approach', 'method', 'year', 'world_player', 'country_3DID_iso']).sum()

# Save output tables
rnd_conso.to_csv(r'C:\Users\Simon\PycharmProjects\rnd-private\rnd_new_approach\benchmark.csv',
                 columns=['rnd_clean'],
                 float_format='%.10f',
                 na_rep='#N/A'
                 )

# print('Consolidate rnd by MNC')

# mtd.get_group_rnd_distribution(
#     parent_ids,
#     parent_rnd,
#     sub_rnd_grouped_w_bvd9
# )

# with open(reg.case_path.joinpath(r'report.json'), 'r') as file:
#     report = json.load(file)
#
# mtd.pprint(report)

