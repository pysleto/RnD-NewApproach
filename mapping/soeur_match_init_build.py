import os
import sys

from pathlib import Path
import datetime

import pandas as pd
import numpy as np

from tabulate import tabulate

from mapping import match_methods as mtd

# Set  DataFrame display options
pd.options.display.max_columns = None
pd.options.display.width = None

root = Path(r'C:\Users\Simon\PycharmProjects\rnd-private')

output_path = root.joinpath(r'mapping\output\soeur_match_update_' + str(datetime.date.today()) + '.csv')

# <editor-fold desc="#00 - Initialisation">

match_dtypes = {
    **{col: str for col in
       ['scoreboard_name', 'soeur_group_id', 'soeur_sub_id', 'soeur_parent_id',
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

match_to_update = pd.read_csv(
    root.joinpath(r'mapping/init/company_table.csv'),
    index_col='soeur_name',
    dtype=match_dtypes
)

new_match = pd.read_csv(
    root.joinpath(r'mapping/init/current_soeur_match_2020-05-02.csv'),
    index_col='soeur_name',
    dtype=match_dtypes
)

new_match = new_match[
    ['step', 'comment', 'scoreboard_name', 'current_orbis_bvd_name', 'current_orbis_bvd_id', 'current_orbis_bvd9',
     'current_is_orbis_sub', 'current_is_orbis_parent', 'current_is_orbis_MNC',
     'current_orbis_sub_bvd_name', 'current_orbis_sub_bvd9', 'current_orbis_parent_bvd_name', 'current_orbis_parent_bvd9',
     'current_orbis_MNC_bvd_name', 'current_orbis_MNC_bvd9',
     'original_bvd_name_FP', 'original_bvd_id_FP', 'current_ratio_name', 'current_ratio_rate', 'ratio_name',
     'ratio_rate', 'partial_ratio_name', 'partial_ratio_rate', 'token_sort_ratio_name', 'token_sort_ratio_rate',
     'token_set_ratio_name', 'token_set_ratio_rate']]

match_to_update = pd.merge(
    match_to_update,
    new_match,
    how='left',
    left_index=True, right_index=True,
    suffixes=(False, False)
)

match_to_update.to_csv(
    output_path,
    float_format='%.10f',
    na_rep='#N/A'
)
# </editor-fold>

# <editor-fold desc="#01 - Confirm or update match on previously identified bvd_name">

# All match names are equal and match rate sum is maxed
step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())

name_query = match_to_update[
    ['ratio_name', 'partial_ratio_name', 'token_sort_ratio_name', 'token_set_ratio_name']
].eq(match_to_update.loc[:, 'current_orbis_bvd_name'], axis=0).all(1)

rate_query = match_to_update[
                 ['ratio_rate', 'partial_ratio_rate', 'token_sort_ratio_rate', 'token_set_ratio_rate']
             ].sum(axis=1) == 400

update = match_to_update.loc[step_query & name_query & rate_query].copy()

update['orbis_bvd_name'] = update.current_orbis_bvd_name
update['step'] = '#01'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# ratio + sort ratios > 80 with matching names
step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())

name_query = match_to_update[
    ['ratio_name', 'token_sort_ratio_name']].eq(match_to_update.loc[:, 'current_orbis_bvd_name'], axis=0).all(1)

rate_query = (match_to_update['ratio_rate'].ge(80)) & (match_to_update['token_sort_ratio_rate'].ge(80))

update = match_to_update.loc[step_query & name_query & rate_query].copy()

update['orbis_bvd_name'] = update.current_orbis_bvd_name
update['step'] = '#01'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# Set ratio = 100 with matching name
step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())

rate_query = match_to_update['token_set_ratio_rate'].eq(100)

name_query = match_to_update['token_set_ratio_name'] == match_to_update['current_orbis_bvd_name']

update = match_to_update.loc[step_query & name_query & rate_query].copy()

update['orbis_bvd_name'] = update.current_orbis_bvd_name
update['step'] = '#01'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# Matching ratio_name
step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())

name_query = match_to_update['current_orbis_bvd_name'] == match_to_update['ratio_name']

update = match_to_update.loc[step_query & name_query].copy()

update['orbis_bvd_name'] = update.current_orbis_bvd_name
update['step'] = '#01'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# Matching partial_ratio_name
step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())

name_query = match_to_update['current_orbis_bvd_name'] == match_to_update['partial_ratio_name']

update = match_to_update.loc[step_query & name_query].copy()

update['orbis_bvd_name'] = update.current_orbis_bvd_name
update['step'] = '#01'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# Matching token_sort_ratio_name
step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())

name_query = match_to_update['current_orbis_bvd_name'] == match_to_update['token_sort_ratio_name']

update = match_to_update.loc[step_query & name_query].copy()

update['orbis_bvd_name'] = update.current_orbis_bvd_name
update['step'] = '#01'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# Matching token_set_ratio_name
step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())

name_query = match_to_update['current_orbis_bvd_name'] == match_to_update['token_set_ratio_name']

update = match_to_update.loc[step_query & name_query].copy()

update['orbis_bvd_name'] = update.current_orbis_bvd_name
update['step'] = '#01'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)
# </editor-fold>

# <editor-fold desc="#02 - Update match on cases where no bvd_name is identified">

# Match rate sum is maxed and names are matching
step_query = (match_to_update.orbis_bvd_name.isna()) & (match_to_update.current_orbis_bvd_name.isna())

name_query = match_to_update[
    ['ratio_name', 'partial_ratio_name', 'token_sort_ratio_name', 'token_set_ratio_name']
].eq(match_to_update.loc[:, 'ratio_name'], axis=0).all(1)

rate_query = match_to_update[
                 ['ratio_rate', 'partial_ratio_rate', 'token_sort_ratio_rate', 'token_set_ratio_rate']
             ].sum(axis=1) == 400

update = match_to_update.loc[step_query & name_query & rate_query].copy()

update['orbis_bvd_name'] = update.ratio_name
update['step'] = '#02'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# Match names are equal, partial + set ratios are maxed and ratio + sort ratios > 80
step_query = (match_to_update.orbis_bvd_name.isna()) & (match_to_update.current_orbis_bvd_name.isna())

name_query = match_to_update[
    ['ratio_name', 'partial_ratio_name', 'token_sort_ratio_name',
     'token_set_ratio_name']].eq(match_to_update.loc[:, 'ratio_name'], axis=0).all(1)

rate_query = (match_to_update['ratio_rate'].ge(80)) & (match_to_update['partial_ratio_rate'].eq(100)) & (
    match_to_update['token_sort_ratio_rate'].ge(80)) & (match_to_update['token_set_ratio_rate'].eq(100))

update = match_to_update.loc[step_query & name_query & rate_query].copy()

update['orbis_bvd_name'] = update.ratio_name
update['step'] = '#02'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# Partial and set ratios are maxed and corresponding names are matching
step_query = (match_to_update.orbis_bvd_name.isna()) & (match_to_update.current_orbis_bvd_name.isna())

name_query = match_to_update['partial_ratio_name'] == match_to_update['token_set_ratio_name']

rate_query = (match_to_update['partial_ratio_rate'].eq(100)) & (match_to_update['token_set_ratio_rate'].eq(100))

update = match_to_update.loc[step_query & name_query & rate_query].copy()

update['orbis_bvd_name'] = update.partial_ratio_name
update['step'] = '#02'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# Set ratios = 100 AND other names are #N/A or matching
step_query = (match_to_update.orbis_bvd_name.isna()) & (match_to_update.current_orbis_bvd_name.isna())

rate_query = match_to_update['token_set_ratio_rate'].eq(100)

update = match_to_update.loc[step_query & rate_query].copy()

update['ratio_name'].mask(
    update['ratio_name'].isna(), update['token_set_ratio_name'], inplace=True
)
update['partial_ratio_name'].mask(
    update['partial_ratio_name'].isna(), update['token_set_ratio_name'], inplace=True
)
update['token_sort_ratio_name'].mask(
    update['token_sort_ratio_name'].isna(), update['token_set_ratio_name'], inplace=True
)

update = update[
    update[
        ['ratio_name', 'partial_ratio_name', 'token_set_ratio_name']].eq(
        update.loc[:, 'token_sort_ratio_name'], axis=0).all(1)
]

update['orbis_bvd_name'] = update.token_sort_ratio_name
update['step'] = '#02'
update['comment'] = 'Perform manual check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)
# </editor-fold>

# <editor-fold desc="#03 - Confirm on previously identified bvd_id">

# Load parent_ids
parent_ids = pd.read_csv(
    root.joinpath(r'cases/2018_GLOBAL/1 - identification - parents.csv'),
    na_values='#N/A',
    dtype={
        col: str for col in ['guo_bvd9', 'bvd9', 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
    }
)

parent_ids.dropna(subset=['company_name'], inplace=True)

# Update if corresponds to a parent
step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd9.isna())

bvd_query = match_to_update.current_orbis_bvd9.isin(parent_ids.bvd9)

update = match_to_update.loc[step_query & bvd_query].copy()

update = pd.merge(
    update,
    parent_ids[['bvd9', 'company_name']],
    left_on='current_orbis_bvd9', right_on='bvd9',
    how='left',
    suffixes=(False, False)
)

update['orbis_bvd_name'] = update.company_name
update['step'] = '#03'
update['comment'] = 'Perform manual check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)

# Load sub_ids
sub_ids = pd.read_csv(
    root.joinpath(r'cases/2018_GLOBAL/1 - identification - subsidiaries.csv'),
    na_values='#N/A',
    dtype={
        col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_NACE_4Dcode']
    }
)

sub_ids.dropna(subset=['sub_company_name'], inplace=True)

sub_ids.drop_duplicates(subset=['sub_bvd9', 'sub_company_name'], inplace=True)

# Update if corresponds to a sub
step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd9.isna())

bvd_query = match_to_update.current_orbis_bvd9.isin(sub_ids.sub_bvd9)

update = match_to_update.loc[step_query & bvd_query].copy()

update = pd.merge(
    update,
    sub_ids[['sub_bvd9', 'sub_company_name']],
    left_on='current_orbis_bvd9', right_on='sub_bvd9',
    how='left',
    suffixes=(False, False)
)

update['orbis_bvd_name'] = update.sub_company_name
update['step'] = '#03'
update['comment'] = 'Perform manual check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)
# </editor-fold>

# <editor-fold desc="#04 - Update all newly matched orbis fields">
match_to_update.reset_index(inplace=True)

match_to_update = pd.merge(
    match_to_update,
    parent_ids[['bvd9', 'company_name']],
    left_on='orbis_bvd_name', right_on='company_name',
    how='left',
    suffixes=(False, False)
)

print(match_to_update.head())
print(match_to_update.describe(include='all'))

match_to_update = pd.merge(
    match_to_update,
    sub_ids[['sub_bvd9', 'sub_company_name']],
    left_on='orbis_bvd_name', right_on='sub_company_name',
    how='left',
    suffixes=(False, False)
)

print(match_to_update.head())
print(match_to_update.describe(include='all'))

match_to_update['is_orbis_parent'] = ~match_to_update['company_name'].isna()
match_to_update.loc[match_to_update.is_orbis_parent, 'orbis_parent_bvd_name'] = match_to_update.company_name
match_to_update.loc[match_to_update.is_orbis_parent, 'orbis_parent_bvd9'] = match_to_update.bvd9

match_to_update['is_orbis_sub'] = ~match_to_update['sub_company_name'].isna()
match_to_update.loc[match_to_update.is_orbis_sub, 'orbis_sub_bvd_name'] = match_to_update.sub_company_name
match_to_update.loc[match_to_update.is_orbis_sub, 'orbis_sub_bvd9'] = match_to_update.sub_bvd9

match_update_cols = ['scoreboard_name', 'soeur_name', 'is_soeur_group', 'soeur_group_id',
                     'is_soeur_sub', 'soeur_sub_id', 'soeur_parent_name', 'soeur_parent_id',
                     'orbis_bvd_name', 'is_orbis_sub', 'is_orbis_parent', 'is_orbis_MNC',
                     'orbis_sub_bvd_name', 'orbis_sub_bvd9', 'orbis_parent_bvd_name', 'orbis_parent_bvd9',
                     'orbis_MNC_bvd_name', 'orbis_MNC_bvd9',
                     'step', 'comment',
                     'current_orbis_bvd_name', 'current_is_orbis_sub', 'current_is_orbis_parent', 'current_is_orbis_MNC',
                     'current_orbis_sub_bvd_name', 'current_orbis_sub_bvd9', 'current_orbis_parent_bvd_name',
                     'current_orbis_parent_bvd9', 'current_orbis_MNC_bvd_name', 'current_orbis_MNC_bvd9',
                      'original_bvd_name_FP', 'original_bvd_id_FP',
                     'current_ratio_name', 'current_ratio_rate', 'ratio_name', 'ratio_rate', 'partial_ratio_name',
                     'partial_ratio_rate', 'token_sort_ratio_name', 'token_sort_ratio_rate', 'token_set_ratio_name',
                     'token_set_ratio_rate']

# Save step file for archive and next match starter
match_to_update.to_csv(
    root.joinpath(r'mapping\output\soeur_match_update_' + str(datetime.date.today()) + '.csv'),
    columns=match_update_cols,
    index=False,
    float_format='%.10f',
    na_rep='#N/A'
)
# </editor-fold>
