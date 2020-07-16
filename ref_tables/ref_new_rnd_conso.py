# Import libraries
import os
import sys

from config import registry as reg

from pathlib import Path
import datetime

import pandas as pd

from ref_tables import ref_methods as mtd

print('Load sub_rnd from file ...')

sub_rnd = pd.read_csv(
    reg.sub['rnd'],
    na_values='#N/A',
    dtype={
        col: str for col in ['bvd9', 'sub_bvd9']
    }
)

print('Load parent_ids from file ...')

parent_ids = pd.read_csv(
    reg.parent['id'],
    na_values='#N/A',
    dtype={
        col: str for col in ['guo_bvd9', 'bvd9', 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
    }
)

print('Load parent_guo_ids from file ...')

parent_guo_ids = pd.read_csv(
    reg.parent['guo'],
    na_values='#N/A',
    dtype={
        col: str for col in ['guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id']
    }
)

print('Load sub_ids from file ...')

sub_ids = pd.read_csv(
    reg.sub['id'],
    na_values='#N/A',
    dtype={
        col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_NACE_4Dcode']
    }
)

print('Load sub_fins from file ...')

sub_fins = pd.read_csv(
    reg.sub['fin'],
    na_values='#N/A',
    dtype={
        col: str for col in ['sub_bvd9', 'sub_bvd_id']
    }
)

print('Consolidate rnd by approach')

rnd_conso = pd.DataFrame()

print('> Prepare sub_rnd ...')

# TODO: Save a full non grouped output file

sub_rnd_grouped = mtd.merge_n_group_sub_rnd(
    sub_rnd.loc[:, ['sub_bvd9', 'bvd9', 'year', 'sub_rnd_clean', 'method']],
    parent_ids.loc[:, ['guo_bvd9', 'bvd9', 'is_listed_company']],
    parent_guo_ids[['guo_bvd9', 'guo_type']],
    sub_ids[['sub_bvd9', 'sub_country_2DID_iso']].drop_duplicates(subset='sub_bvd9'),
    sub_fins
)

sub_rnd_grouped.rename(columns={
    'sub_country_3DID_iso': 'country_3DID_iso',
    'sub_world_player': 'world_player',
    'sub_rnd_clean': 'rnd_clean'
}, inplace=True)

# sub_rnd_grouped_columns = ['year', 'country_3DID_iso', 'world_player', 'guo_type', 'type',
#        'cluster', 'method', 'rnd_clean', 'approach', 'technology', 'priority',
#        'action']

sub_rnd_grouped.to_csv(
    reg.project_path.joinpath(r'ref_tables', 'ORBIS_' + reg.use_case, reg.use_case + '_'
                                 + str(datetime.date.today()) + '_rnd_by_country_cluster_n_method.csv'),
    # columns = regtech_rnd_cols,
    float_format = '%.10f',
    index = False,
    na_rep = '#N/A'
    )

