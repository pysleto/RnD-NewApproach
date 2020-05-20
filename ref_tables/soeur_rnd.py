# Import libraries
import os
import sys

from pathlib import Path
from datetime import datetime

import pandas as pd

from ref_tables import ref_methods as mtd
import init_config as cfg

# Set  DataFrame display options
pd.options.display.max_columns = None
pd.options.display.width = None

# Load config files
reg = cfg.load_my_registry()

soeur_version = 'SOEUR_RnD_2019b_20200309'

if not reg['project_root'].joinpath(r'ref_tables', 'SOEUR_RnD', soeur_version + '.csv').exists():
    mtd.update_n_format_soeur_rnd(soeur_version)

soeur_rnd = pd.read_csv(
    reg['project_root'].joinpath(r'ref_tables', 'SOEUR_RnD', soeur_version + '.csv'),
    error_bad_lines=False, encoding='UTF-8',
    dtype={
        **{col: object for col in
           ['soeur_group_id', 'soeur_group_name', 'soeur_group_bvd_id', 'group_country_2DID_soeur',
            'soeur_sub_id',
            'soeur_sub_name', 'sub_country_2DID_soeur', 'sub_NUTS1', 'sub_NUTS2', 'sub_NUTS3', 'technology',
            'action',
            'priority', 'group_country_2DID_iso', 'group_world_player', 'sub_country_2DID_iso',
            'sub_world_player']},
        **{col: bool for col in []},
        **{col: float for col in ['sector_UC', 'group_UC', 'rnd_group_UC', 'rnd_clean']}
    },
    parse_dates=['year'], date_parser=lambda y: datetime.strptime(y, '%Y').strftime('%Y')
).rename(columns={
    'group_country_2DID_iso': 'soeur_group_country_2DID_iso',
    'sub_country_2DID_iso': 'soeur_sub_country_2DID_iso',
    'group_world_player': 'soeur_group_world_player',
    'sub_world_player': 'soeur_sub_world_player'
}
)

print('Save output table')

print('... at subsidiary level')

# Save output tables at subsidiary level

# soeur subs identification tables
subs_ids = soeur_rnd[['soeur_sub_id', 'soeur_sub_name', 'sub_NUTS1', 'sub_NUTS2', 'sub_NUTS3',
                      'soeur_sub_country_2DID_iso', 'soeur_group_id', 'soeur_group_name'
                      ]].drop_duplicates().dropna(subset=['soeur_sub_id'])

subs_ids.to_csv(reg['project_root'].joinpath(r'ref_tables', 'SOEUR_RnD', soeur_version + '_sub_ids.csv'),
                index=False, na_rep='#N/A', encoding='UTF-8'
                )

sub_rnd_cols = ['soeur_sub_id', 'soeur_sub_name', 'sub_NUTS1', 'sub_NUTS2', 'sub_NUTS3', 'soeur_sub_country_2DID_iso',
                'soeur_sub_world_player', 'year', 'rnd_clean']

soeur_rnd.to_csv(reg['project_root'].joinpath(r'ref_tables', 'SOEUR_RnD', soeur_version + '_by_sub.csv'),
                 columns=sub_rnd_cols,
                 float_format='%.10f',
                 index=False,
                 na_rep='#N/A'
                 )

print('... at group level')

# Save output tables at group level

# Load soeur_to_orbis reference table
company = pd.read_csv(
    reg['company'],
    error_bad_lines=False, encoding='UTF-8',
    dtype={
        **{col: str for col in
           ['scoreboard_name', 'soeur_name', 'soeur_group_id', 'soeur_sub_id', 'soeur_parent_id', 'soeur_parent_name',
            'orbis_bvd_name', 'orbis_sub_bvd_name', 'orbis_sub_bvd9', 'orbis_parent_bvd_name', 'orbis_parent_bvd9',
            'orbis_MNC_bvd_name', 'orbis_MNC_bvd9', 'original_bvd_name_FP', 'original_bvd_id_FP']},
        **{col: bool for col in ['is_soeur_group', 'is_soeur_sub', 'is_orbis_sub', 'is_orbis_parent', 'is_orbis_MNC']}
    }
)

parent_ids = pd.read_csv(
    reg['parent']['id'],
    na_values='#N/A', encoding='UTF-8',
    dtype={
        **{col: str for col in
           ['company_name', 'NACE_desc', 'country_2DID_iso', 'country_3DID_iso', 'world_player', 'bvd9', 'bvd_id',
            'legal_entity_id', 'guo_bvd9', 'NACE_4Dcode']},
        **{col: bool for col in ['is_listed_company', 'is_guo50']},
        **{col: int for col in ['subs_n']},
    }
)

# soeur group identification tables
soeur_rnd = pd.merge(
    soeur_rnd,
    company[['soeur_name', 'orbis_parent_bvd_name', 'orbis_parent_bvd9']],
    left_on='soeur_group_name', right_on='soeur_name',
    how='left',
    suffixes=(False, False)
)

soeur_rnd = pd.merge(
    soeur_rnd,
    parent_ids[['bvd9', 'country_2DID_iso', 'world_player']],
    left_on='orbis_parent_bvd9', right_on='bvd9',
    how='left',
    suffixes=(False, False)
).drop(columns=['bvd9']).rename(
    columns={'country_2DID_iso': 'orbis_country_2DID_iso', 'world_player': 'orbis_world_player'})

group_ids = soeur_rnd[
    ['soeur_group_id', 'soeur_group_name', 'soeur_group_country_2DID_iso', 'orbis_parent_bvd_name', 'orbis_parent_bvd9',
     'orbis_country_2DID_iso', 'orbis_world_player']].copy()

group_ids.drop_duplicates(subset=['soeur_group_name'], inplace=True)

group_ids['is_mapped'] = (~group_ids.soeur_group_name.isna()) & (~group_ids.orbis_parent_bvd_name.isna())
group_ids['is_country_match'] = group_ids.soeur_group_country_2DID_iso == group_ids.orbis_country_2DID_iso

group_ids.to_csv(reg['project_root'].joinpath(r'ref_tables', 'SOEUR_RnD', soeur_version + '_group_ids.csv'),
                 index=False,
                 na_rep='#N/A', encoding='UTF-8'
                 )

# soeur group exposure and rnd estimates
group_rnd_cols = ['soeur_group_id', 'soeur_group_name', 'soeur_group_country_2DID_iso', 'soeur_group_world_player',
                  'orbis_parent_bvd_name', 'orbis_parent_bvd9', 'orbis_country_2DID_iso',
                  'year', 'sector_UC', 'group_UC', 'rnd_group_UC', 'rnd_clean']

soeur_rnd_by_group = soeur_rnd.groupby(group_rnd_cols[:-4]).agg({
    **{col: 'mean' for col in ['sector_UC', 'group_UC', 'rnd_group_UC']},
    'rnd_clean': 'sum'
})

soeur_rnd_by_group.reset_index(inplace=True)

soeur_rnd_by_group.to_csv(reg['project_root'].joinpath(r'ref_tables', 'SOEUR_RnD', soeur_version + '_by_group.csv'),
                          columns=group_rnd_cols,
                          float_format='%.10f',
                          index=False,
                          na_rep='#N/A'
                          )

print('... at region level')

regtech_rnd_cols = ['year', 'soeur_sub_country_2DID_iso', 'soeur_sub_world_player', 'technology', 'action', 'priority',
                    'rnd_clean']

soeur_rnd_by_region_n_tech = soeur_rnd.groupby(regtech_rnd_cols[:-1]).sum()

soeur_rnd_by_region_n_tech.reset_index(inplace=True)

soeur_rnd_by_region_n_tech.to_csv(
    reg['project_root'].joinpath(r'ref_tables', 'SOEUR_RnD', soeur_version + '_by_region_n_tech.csv'),
    columns=regtech_rnd_cols,
    float_format='%.10f',
    index=False,
    na_rep='#N/A'
    )
