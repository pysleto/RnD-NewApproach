# Import libraries
import sys
import os

from pathlib import Path
import datetime

import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from tabulate import tabulate

from data_input import file_loader as load

import init_config as cfg

# Load config files
reg = cfg.load_my_registry()

# Set  DataFrame display options
pd.options.display.max_columns = None
pd.options.display.width = None

company_level = 'sub'  # 'parent'

output_path = reg['project_root'].joinpath(
    r'mapping\output\soeur_' + company_level + '_match_update_' + str(datetime.date.today()) + '.csv'
)

# <editor-fold desc="#01 - Load current mapping">
match_dtypes = {
    **{col: str for col in
       ['scoreboard_name', 'soeur_name', 'soeur_group_id', 'soeur_sub_id', 'soeur_parent_id',
        'orbis_bvd_name', 'orbis_sub_bvd_name', 'orbis_sub_bvd9', 'orbis_parent_bvd_name', 'orbis_parent_bvd9',
        'orbis_MNC_bvd_name', 'orbis_MNC_bvd9',
        'step', 'comment',
        'current_orbis_bvd_name', 'current_orbis_bvd_id', 'current_orbis_bvd9', 'current_orbis_sub_bvd_name',
        'current_orbis_sub_bvd9', 'current_orbis_parent_bvd_name', 'current_orbis_parent_bvd9',
        'current_orbis_MNC_bvd_name', 'current_orbis_MNC_bvd9',
        'original_bvd_name_FP', 'original_bvd_id_FP',
        'current_ratio_name', 'ratio_name', 'partial_ratio_name', 'token_sort_ratio_name', 'token_set_ratio_name']},
    **{col: bool for col in ['is_soeur_group', 'is_soeur_sub', 'is_orbis_sub', 'is_orbis_parent', 'is_orbis_MNC',
                             'current_is_orbis_sub', 'current_is_orbis_parent', 'current_is_orbis_MNC']},
    **{col: float for col in
       ['current_ratio_rate', 'ratio_rate', 'partial_ratio_rate', 'token_sort_ratio_rate', 'token_set_ratio_rate']}
}

# TODO: Update from remote reference table
current_map = pd.read_csv(
    reg['project_root'].joinpath(r'mapping/init/current_soeur_sub_match_init.csv'),
    na_values='#N/A',
    dtype=match_dtypes,
    encoding='UTF-8'
)

if not reg['project_root'].joinpath(r'mapping/current_match.csv').exists():
    ref_ids = current_map.loc[current_map.orbis_bvd_name.isna(), ['soeur_name']].copy()
    ref_ids.drop_duplicates(inplace=True)

    ref_ids.set_index('soeur_name', inplace=True)

    ref_ids_to_search = ref_ids
else:
    ref_ids = pd.read_csv(
        reg['project_root'].joinpath(r'mapping/current_match.csv'),
        na_values='#N/A',
        index_col='soeur_name',
        dtype={
            **{col: float for col in ['ratio_rate', 'partial_ratio_rate', 'token_sort_ratio_rate',
                                      'token_set_ratio_rate']},
            **{col: str for col in ['ratio_name', 'partial_ratio_name', 'token_sort_ratio_name',
                                    'token_set_ratio_name']},
        },
        encoding='UTF-8'
    )

    ref_ids_to_search = ref_ids[ref_ids.ratio_name.isna()]

to_search_for_count = ref_ids_to_search.index.value_counts().sum()
# </editor-fold>


# <editor-fold desc="#02 - Load new company names and ids to map with">

# # CASE: ORBIS parents
# parent_ids = pd.read_csv(
#     root.joinpath(r'cases/2018_GLOBAL/1 - identification - parents.csv'),
#     na_values='n.a.',
#     dtype={
#         col: str for col in ['bvd9', 'guo_bvd9' 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
#     }
# )

# CASE: ORBIS subs
sub_ids = pd.read_csv(
    reg['sub']['id'],
    na_values='#N/A',
    dtype={
        col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_NACE_4Dcode']
    },
    encoding='UTF-8'
)

sub_rnd = pd.read_csv(
    reg['sub']['rnd'],
    na_values='#N/A',
    dtype={
        col: str for col in ['bvd9', 'sub_bvd9']
    }
)

sub_ids = sub_ids.loc[sub_ids.sub_bvd9.isin(sub_rnd.sub_bvd9), ['sub_company_name']]

sub_ids.rename(columns={'sub_company_name': 'company_name'}, inplace=True)
sub_ids.drop_duplicates(inplace=True)
sub_ids.dropna(inplace=True)

# Generic handler
to_search_on = sub_ids
# </editor-fold>

for count, name_to_match in enumerate(ref_ids_to_search.index.values[1:], start=1):

    # is_match = False

    try:
        print(
            tabulate([[
                # str(i_match) + ' matches',
                str(count) + ' / ' + str(to_search_for_count) + ' steps',
                # 'Is a match: ' + str(is_match),
                'Input: ' + str(name_to_match),
                # 'Outputs:' + match[0],
                # '(' + str(match[1]) + ')'
            ]], tablefmt="plain")
        )
    except UnicodeEncodeError:
        print('Yo Mama')
        pass

    print('... fuzz.ratio')

    ratio_match = process.extractOne(
        name_to_match, to_search_on['company_name'], scorer=fuzz.ratio
    )

    print('... fuzz.partial_ratio')

    partial_ratio_match = process.extractOne(
        name_to_match, to_search_on['company_name'], scorer=fuzz.partial_ratio
    )

    print('... fuzz.token_sort_ratio')

    token_sort_ratio_match = process.extractOne(
        name_to_match, to_search_on['company_name'], scorer=fuzz.token_sort_ratio
    )

    print('... fuzz.token_set_ratio')

    token_set_ratio_match = process.extractOne(
        name_to_match, to_search_on['company_name'], scorer=fuzz.token_set_ratio
    )

    # fuzz.ratio
    # fuzz.partial_ratio
    # fuzz.token_sort_ratio
    # fuzz.token_set_ratio

    ref_ids.loc[name_to_match, 'ratio_name'] = ratio_match[0]
    ref_ids.loc[name_to_match, 'ratio_rate'] = ratio_match[1]
    ref_ids.loc[name_to_match, 'partial_ratio_name'] = partial_ratio_match[0]
    ref_ids.loc[name_to_match, 'partial_ratio_rate'] = partial_ratio_match[1]
    ref_ids.loc[name_to_match, 'token_sort_ratio_name'] = token_sort_ratio_match[0]
    ref_ids.loc[name_to_match, 'token_sort_ratio_rate'] = token_sort_ratio_match[1]
    ref_ids.loc[name_to_match, 'token_set_ratio_name'] = token_set_ratio_match[0]
    ref_ids.loc[name_to_match, 'token_set_ratio_rate'] = token_set_ratio_match[1]

    ref_ids.to_csv(
        reg['project_root'].joinpath(r'mapping\current_match.csv'),
        float_format='%.10f',
        na_rep='#N/A'
    )

# current_map.set_index('current_bvd_name', inplace=True)
# current_map.set_index('soeur_group_name', inplace=True)

ref_ids.reset_index(inplace=True)

match_update = pd.merge(
    current_map,
    ref_ids,
    left_on='soeur_name', right_on='soeur_name',
    how='left',
    suffixes=(False, False)
)

match_update['step'] = match_update['comment'] = match_update['current_ratio_name'] = pd.Series([], dtype=object)

match_update['current_ratio_rate'] = pd.Series([], dtype=float)

for col in ['is_orbis_sub', 'is_orbis_parent', 'is_orbis_MNC']:
    match_update['current_' + col] = match_update[col]
    match_update[col] = False

for col in ['orbis_bvd_name', 'orbis_sub_bvd_name', 'orbis_sub_bvd9', 'orbis_parent_bvd_name', 'orbis_parent_bvd9',
            'orbis_MNC_bvd_name', 'orbis_MNC_bvd9']:
    match_update['current_' + col] = match_update[col]
    match_update[col] = pd.Series([], dtype=object)
#
# print(list(match_update.columns))
#
match_update.loc[
:, ['current_ratio_rate', 'ratio_rate', 'partial_ratio_rate', 'token_sort_ratio_rate', 'token_set_ratio_rate']
].fillna(0, inplace=True)

match_update_cols = ['scoreboard_name', 'soeur_name', 'is_soeur_group', 'soeur_group_id',
                     'is_soeur_sub', 'soeur_sub_id', 'soeur_parent_name', 'soeur_parent_id',
                     'orbis_bvd_name', 'is_orbis_sub', 'is_orbis_parent', 'is_orbis_MNC',
                     'orbis_sub_bvd_name', 'orbis_sub_bvd9', 'orbis_parent_bvd_name', 'orbis_parent_bvd9',
                     'orbis_MNC_bvd_name', 'orbis_MNC_bvd9',
                     'original_bvd_name_FP', 'original_bvd_id_FP',
                     'step', 'comment',
                     'current_orbis_bvd_name', 'current_is_orbis_sub', 'current_is_orbis_parent',
                     'current_is_orbis_MNC',
                     'current_orbis_sub_bvd_name', 'current_orbis_sub_bvd9', 'current_orbis_parent_bvd_name',
                     'current_orbis_parent_bvd9', 'current_orbis_MNC_bvd_name', 'current_orbis_MNC_bvd9',
                     'current_ratio_name', 'current_ratio_rate', 'ratio_name', 'ratio_rate', 'partial_ratio_name',
                     'partial_ratio_rate', 'token_sort_ratio_name', 'token_sort_ratio_rate', 'token_set_ratio_name',
                     'token_set_ratio_rate']

match_update.to_csv(
    output_path,
    columns=match_update_cols,
    index=False,
    float_format='%.10f',
    na_rep='#N/A'
)
