# Import libraries

import pandas as pd
import datetime
import json

from rnd_new_approach import rnd_methods as mtd
import config as cfg

# TODO: transfer in a specific report.py file and transfer report.py in method.py
# TODO: How does disclosed rnd and oprev in subs compare with rnd and oprev in parents
# TODO: How much disclosed sub rnd is embedded in keyword matching subs and not accounted for in subs final rnd
# TODO: How much disclosed parent rnd is embedded in parent that have a potential keyword match but have no subsidiaries
# TODO: Select top 10 parents in each cluster and consolidate oprev, exposure and rnd trends over range_ys
# TODO: Distribution and cumulative distribution functions for parent and subs (rnd x oprev or market cap?) by world_player
# TODO: Ex-post exposure global and by world_player

# <editor-fold desc="#0 - Initialisation">

# Set  dataframe display options
pd.options.display.max_columns = None
pd.options.display.width = None

# Load config files
reg = cfg.init()

# Load keywords for activity screening
with open(reg['rnd_root'].joinpath(r'keywords.json'), 'r') as file:
    keywords = json.load(file)

categories = list(keywords.keys())

rnd_cluster_cats = [cat for cat in categories if cat not in ['generation', 'rnd']]

# Define data ranges
range_ys = {
    'rnd_ys': ['rnd_y' + str(YY) for YY in range(int(reg['year_first'][-2:]), int(reg['year_last'][-2:]) + 1)],
    'oprev_ys': ['op_revenue_y' + str(YY) for YY in
                 range(int(reg['year_first'][-2:]), int(reg['year_last'][-2:]) + 1)],
    'LY': str(reg['year_last'])[-2:]
}

# Import mapping tables
print('Read country mapping table ...')

country_ref = pd.read_csv(reg['country'], error_bad_lines=False)

# </editor-fold>

# <editor-fold desc="#1 - Load rnd_main ouputs">
print('Load rnd estimates')

print('Load sub_rnd from file ...')

sub_rnd = pd.read_csv(
    reg['sub']['rnd'],
    na_values='#N/A',
    dtype={
        col: str for col in ['bvd9', 'sub_bvd9']
    }
)

print('Load parent_ids from file ...')

parent_ids = pd.read_csv(
    reg['parent']['id'],
    na_values='#N/A',
    dtype={
        col: str for col in ['guo_bvd9', 'bvd9', 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
    }
)

print('Load parent_guo_ids from file ...')

parent_guo_ids = pd.read_csv(
    reg['parent']['guo'],
    na_values='#N/A',
    dtype={
        col: str for col in ['guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id']
    }
)

print('Load sub_ids from file ...')

sub_ids = pd.read_csv(
    reg['sub']['id'],
    na_values='#N/A',
    dtype={
        col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_NACE_4Dcode']
    }
)

print('Load sub_fins from file ...')

sub_fins = pd.read_csv(
    reg['sub']['fin'],
    na_values='#N/A',
    dtype={
        col: str for col in ['sub_bvd9', 'sub_bvd_id']
    }
)
# </editor-fold>

print('Consolidate rnd by approach')

rnd_conso = pd.DataFrame()

print('> Prepare sub_rnd ...')

sub_rnd_grouped = mtd.merge_n_group_sub_rnd(
    reg,
    rnd_cluster_cats,
    sub_rnd.loc[:, ['sub_bvd9', 'bvd9', 'year', 'sub_rnd_clean', 'method']],
    parent_ids.loc[:, ['guo_bvd9', 'bvd9', 'is_listed_company']],
    parent_guo_ids[['guo_bvd9', 'guo_type']],
    sub_ids[['sub_bvd9', 'sub_country_2DID_iso']].drop_duplicates(subset='sub_bvd9'),
    country_ref,
    sub_fins
)

sub_rnd_grouped.rename(columns={
    'sub_country_3DID_iso': 'country_3DID_iso',
    'sub_world_player': 'world_player',
    'sub_rnd_clean': 'rnd_clean'
}, inplace=True)

# print('> Prepare soeur_rnd ...')
#
# soeur_rnd_grouped = mtd.load_n_group_soeur_rnd(reg)
#
# soeur_rnd_grouped.rename(columns={
#     'sub_country_3DID_iso': 'country_3DID_iso',
#     'sub_world_player': 'world_player',
#     'sub_rnd_clean': 'rnd_clean'
# }, inplace=True)
#
# print('> Prepare mnc_rnd ...')
#
# mnc_rnd_grouped = mtd.load_n_group_MNC_rnd(reg)
#
# mnc_rnd_grouped.rename(columns={
#     'group_country_3DID_iso': 'country_3DID_iso',
#     'group_world_player': 'world_player',
#     'group_rnd_clean': 'rnd_clean'
# }, inplace=True)
#
# print('> Consolidated dataframe ...')
#
# rnd_conso_cols = ['approach', 'method', 'year', 'sub_rnd_clean', 'guo_type', 'type', 'sub_world_player',
#                   'sub_country_3DID_iso', 'cluster', 'technology', 'priority', 'action']
#
# # TODO : Integrate embedded in MNC rnd
# rnd_conso = rnd_conso.append(soeur_rnd_grouped)

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
#     reg,
#     reg,
#     keywords,
#     parent_ids,
#     parent_rnd,
#     sub_rnd_grouped_w_bvd9
# )

# with open(reg['CASE_ROOT'].joinpath(r'report.json'), 'r') as file:
#     report = json.load(file)
#
# mtd.pprint(report, reg)

