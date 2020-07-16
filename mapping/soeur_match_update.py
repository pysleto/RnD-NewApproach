import datetime

import pandas as pd

from config import registry as reg

from mapping import match_methods as mtd

company_level = 'sub'  # 'parent'

country_merge = 'country_2DID_iso'

if company_level == 'sub':
    country_merge = 'sub_' + country_merge

new_match_path = reg.project_path.joinpath(r'mapping\output\soeur_sub_match_update_2020-05-21.csv')

output_path = reg.project_path.joinpath(
    r'mapping\output\soeur_' + company_level + '_match_update_' + str(datetime.date.today()) + '.csv'
)

match_dtypes = {
    **{col: str for col in
       ['scoreboard_name', 'soeur_group_id', 'soeur_sub_id', 'soeur_sub_country_2DID_iso', 'soeur_parent_id',
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

# <editor-fold desc="#00 - Load ORBIS parent_ids and sub_ids ">
# Load parent_ids
parent_ids = pd.read_csv(
    reg.parent['id'],
    na_values='#N/A',
    dtype={
        col: str for col in ['company_name', 'guo_bvd9', 'bvd9', 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
    }
).rename(columns={'country_2DID_iso': 'parent_country_2DID_iso'})

parent_ids.set_index('company_name', drop=False, inplace=True)

parent_ids.drop_duplicates(inplace=True)

# Load sub_ids
sub_ids = pd.read_csv(
    reg.sub['id'],
    na_values='#N/A',
    dtype={
        col: str for col in ['sub_company_name', 'bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id',
                             'sub_NACE_4Dcode']
    }
).rename(columns={'bvd9': 'sub_parent_bvd9', 'sub_country_2DID_iso': 'sub_country_2DID_iso'})

sub_ids.set_index('sub_company_name', drop=False, inplace=True)

sub_ids.drop_duplicates(inplace=True)
# </editor-fold>

# TODO: Process to update (load and save) with reference table
# <editor-fold desc="#01 - Load reference table to update">
match_to_update_cols = ['is_soeur_group', 'soeur_group_id', 'is_soeur_sub', 'soeur_sub_id', 'soeur_country_2DID_iso',
                        'soeur_parent_name', 'soeur_parent_id',
                        'scoreboard_name',
                        'orbis_bvd_name', 'is_orbis_sub', 'is_orbis_parent',
                        # 'is_orbis_MNC',
                        'orbis_sub_bvd_name', 'orbis_sub_bvd9', 'orbis_sub_country_2DID_iso',
                        'orbis_parent_bvd_name', 'orbis_parent_bvd9', 'orbis_parent_country_2DID_iso',
                        # 'orbis_MNC_bvd_name', 'orbis_MNC_bvd9',
                        'original_bvd_name_FP', 'original_bvd_id_FP']

match_to_update = pd.read_csv(
    reg.project_path.joinpath(r'mapping/init/current_soeur_' + company_level + '_match_init.csv'),
    dtype=match_dtypes,
    encoding='UTF-8'
)

# match_to_update.set_index(['soeur_name', 'soeur_country_2DID_iso'], drop=False, inplace=True)

# check = match_to_update.duplicated(subset=['soeur_name'], keep=False)
#
# if not check.empty:
#     match_to_update[check].to_csv(
#         reg.project_path.joinpath(r'mapping\check\match_to_update.csv'),
#         float_format='%.10f',
#         na_rep='#N/A'
#     )
# else:
#     print('no match_to_update.duplicated')

match_to_update.drop_duplicates(inplace=True)
# </editor-fold>

# <editor-fold desc="#02 - Load new matches and populate orbis match country">
new_match_cols = ['step', 'comment',
                  # 'current_orbis_bvd_name', 'current_is_orbis_sub', 'current_is_orbis_parent', 'current_is_orbis_MNC',
                  # 'current_orbis_sub_bvd_name', 'current_orbis_sub_bvd9', 'current_orbis_parent_bvd_name',
                  # 'current_orbis_parent_bvd9', 'current_orbis_MNC_bvd_name', 'current_orbis_MNC_bvd9',
                  # 'current_ratio_name', 'current_ratio_rate',
                  'ratio_name', 'ratio_rate', 'partial_ratio_name', 'partial_ratio_rate', 'token_sort_ratio_name',
                  'token_sort_ratio_rate', 'token_set_ratio_name', 'token_set_ratio_rate']

new_match = pd.read_csv(
    new_match_path,
    dtype=match_dtypes
)

# new_match.set_index('soeur_name', drop=False, inplace=True)

new_match = new_match[['soeur_name'] + new_match_cols]

new_match.drop_duplicates(inplace=True)

new_match = pd.merge(
    new_match,
    parent_ids[['company_name', 'bvd9', 'parent_country_2DID_iso']],
    left_on='ratio_name', right_index=True,
    how='left',
    suffixes=(False, False)
).drop_duplicates()

new_match = pd.merge(
    new_match,
    sub_ids[['sub_company_name', 'sub_country_2DID_iso']],  # 'sub_bvd9', 'sub_parent_bvd9',
    left_on='ratio_name', right_index=True,
    how='left',
    suffixes=(False, False)
).drop_duplicates()

match_to_update = pd.merge(
    match_to_update,
    new_match,
    how='inner',
    left_on=['soeur_name', 'soeur_country_2DID_iso'], right_on=['soeur_name', country_merge],
    suffixes=(False, False),
    validate='1:1'
).drop_duplicates()

match_to_update.to_csv(
    output_path,
    index=False,
    float_format='%.10f',
    na_rep='#N/A'
)
# </editor-fold>

# <editor-fold desc="#03 - Match update">
match_to_update.set_index(['soeur_name', 'soeur_country_2DID_iso'], inplace=True)

# With ratio > 80, matching first letters and same country
step_query = match_to_update.orbis_bvd_name.isna()

name_query = match_to_update['ratio_name'].str[:4] == match_to_update.index.get_level_values(0).str[:4]

# rate_query = match_to_update['ratio_rate'].ge(80)

rate_query = (match_to_update['ratio_rate'].ge(80)) & (
    match_to_update[['partial_ratio_rate', 'token_sort_ratio_rate', 'token_set_ratio_rate']].eq(100, axis=0).any(1)
)

update = match_to_update.loc[step_query & name_query & rate_query].copy()

update['orbis_bvd_name'] = update.ratio_name
update['step'] = '#01'
update['comment'] = 'Perform random check'

match_to_update = mtd.update_current_match(
    match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
    update[['orbis_bvd_name', 'step', 'comment']],
    output_path,
    match_dtypes
)
# </editor-fold>

# <editor-fold desc="#03b - Draft update rules">

# # Match names are equal, partial & set ratios are maxed and ratio & sort ratios > 80
# step_query = match_to_update.orbis_bvd_name.isna()
#
# name_query = (
#                  match_to_update[
#                      ['partial_ratio_name', 'token_sort_ratio_name', 'token_set_ratio_name']
#                  ].eq(match_to_update.loc[:, 'ratio_name'], axis=0).all(1)
#              ) & (match_to_update['ratio_name'].str[:2] == match_to_update.index.str[:2])
#
# rate_query = (match_to_update['ratio_rate'].ge(80)) & (
#     match_to_update['partial_ratio_rate'].ge(80)) & (
#     match_to_update['token_sort_ratio_rate'].ge(80)) & (
#     match_to_update['token_set_ratio_rate'].ge(80))
#
# update = match_to_update.loc[step_query & name_query & rate_query].copy()
#
# update['orbis_bvd_name'] = update.ratio_name
# update['step'] = '#01'
# update['comment'] = 'Perform random check'
#
# match_to_update = mtd.update_current_match(
#     match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
#     update[['orbis_bvd_name', 'step', 'comment']],
#     output_path,
#     match_dtypes
# )

# # ratio + sort ratios > 80 with matching names
# step_query = match_to_update.orbis_bvd_name.isna()
#
# name_query = match_to_update['ratio_name'] == match_to_update['token_sort_ratio_name']
#
# rate_query = (match_to_update['ratio_rate'].ge(80)) & (match_to_update['token_sort_ratio_rate'].ge(80))
#
# update = match_to_update.loc[step_query & name_query & rate_query].copy()
#
# update['orbis_bvd_name'] = update.ratio_name
# update['step'] = '#02'
# update['comment'] = 'Perform random check'
#
# match_to_update = mtd.update_current_match(
#     match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
#     update[['orbis_bvd_name', 'step', 'comment']],
#     output_path,
#     match_dtypes
# )
#
# # Partial and set ratios are maxed and corresponding names are matching
# step_query = match_to_update.orbis_bvd_name.isna()
#
# name_query = match_to_update['partial_ratio_name'] == match_to_update['token_set_ratio_name']
#
# rate_query = (match_to_update['partial_ratio_rate'].eq(100)) & (match_to_update['token_set_ratio_rate'].eq(100))
#
# update = match_to_update.loc[step_query & name_query & rate_query].copy()
#
# update['orbis_bvd_name'] = update.partial_ratio_name
# update['step'] = '#03'
# update['comment'] = 'Perform random check'
#
# match_to_update = mtd.update_current_match(
#     match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
#     update[['orbis_bvd_name', 'step', 'comment']],
#     output_path,
#     match_dtypes
# )
#
# # Set ratio = 100
# step_query = match_to_update.orbis_bvd_name.isna()
#
# rate_query = match_to_update['token_set_ratio_rate'].eq(100)
#
# update = match_to_update.loc[step_query & rate_query].copy()
#
# update['orbis_bvd_name'] = update.token_set_ratio_name
# update['step'] = '#01'
# update['comment'] = 'Perform random check'
#
# match_to_update = mtd.update_current_match(
#     match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
#     update[['orbis_bvd_name', 'step', 'comment']],
#     output_path,
#     match_dtypes
# )
#
# # Matching ratio_name
# step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())
#
# name_query = match_to_update['current_orbis_bvd_name'] == match_to_update['ratio_name']
#
# update = match_to_update.loc[step_query & name_query].copy()
#
# update['orbis_bvd_name'] = update.current_orbis_bvd_name
# update['step'] = '#01'
# update['comment'] = 'Perform random check'
#
# match_to_update = mtd.update_current_match(
#     match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
#     update[['orbis_bvd_name', 'step', 'comment']],
#     output_path,
#     match_dtypes
# )
#
# # Matching partial_ratio_name
# step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())
#
# name_query = match_to_update['current_orbis_bvd_name'] == match_to_update['partial_ratio_name']
#
# update = match_to_update.loc[step_query & name_query].copy()
#
# update['orbis_bvd_name'] = update.current_orbis_bvd_name
# update['step'] = '#01'
# update['comment'] = 'Perform random check'
#
# match_to_update = mtd.update_current_match(
#     match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
#     update[['orbis_bvd_name', 'step', 'comment']],
#     output_path,
#     match_dtypes
# )
#
# # Matching token_sort_ratio_name
# step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())
#
# name_query = match_to_update['current_orbis_bvd_name'] == match_to_update['token_sort_ratio_name']
#
# update = match_to_update.loc[step_query & name_query].copy()
#
# update['orbis_bvd_name'] = update.current_orbis_bvd_name
# update['step'] = '#01'
# update['comment'] = 'Perform random check'
#
# match_to_update = mtd.update_current_match(
#     match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
#     update[['orbis_bvd_name', 'step', 'comment']],
#     output_path,
#     match_dtypes
# )
#
# # Matching token_set_ratio_name
# step_query = (match_to_update.orbis_bvd_name.isna()) & (~match_to_update.current_orbis_bvd_name.isna())
#
# name_query = match_to_update['current_orbis_bvd_name'] == match_to_update['token_set_ratio_name']
#
# update = match_to_update.loc[step_query & name_query].copy()
#
# update['orbis_bvd_name'] = update.current_orbis_bvd_name
# update['step'] = '#01'
# update['comment'] = 'Perform random check'
#
# match_to_update = mtd.update_current_match(
#     match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
#     update[['orbis_bvd_name', 'step', 'comment']],
#     output_path,
#     match_dtypes
# )
#
# # Set ratios = 100 AND other names are #N/A or matching
# step_query = (match_to_update.orbis_bvd_name.isna()) & (match_to_update.current_orbis_bvd_name.isna())
#
# rate_query = match_to_update['token_set_ratio_rate'].eq(100)
#
# update = match_to_update.loc[step_query & rate_query].copy()
#
# update['ratio_name'].mask(
#     update['ratio_name'].isna(), update['token_set_ratio_name'], inplace=True
# )
# update['partial_ratio_name'].mask(
#     update['partial_ratio_name'].isna(), update['token_set_ratio_name'], inplace=True
# )
# update['token_sort_ratio_name'].mask(
#     update['token_sort_ratio_name'].isna(), update['token_set_ratio_name'], inplace=True
# )
#
# update = update[
#     update[
#         ['ratio_name', 'partial_ratio_name', 'token_set_ratio_name']].eq(
#         update.loc[:, 'token_sort_ratio_name'], axis=0).all(1)
# ]
#
# update['orbis_bvd_name'] = update.token_sort_ratio_name
# update['step'] = '#02'
# update['comment'] = 'Perform manual check'
#
# match_to_update = mtd.update_current_match(
#     match_to_update[match_to_update.orbis_bvd_name.isna()].index.value_counts().sum(),
#     update[['orbis_bvd_name', 'step', 'comment']],
#     output_path,
#     match_dtypes
# )
# </editor-fold>

# <editor-fold desc="#04 - Update all newly matched orbis fields">
match_to_update.reset_index(drop=False, inplace=True)

match_to_update['is_orbis_parent'] = ~match_to_update['company_name'].isna()

match_to_update.loc[match_to_update.is_orbis_parent, 'orbis_parent_bvd_name'] = match_to_update.company_name

match_to_update.loc[
    match_to_update.is_orbis_parent, 'orbis_parent_bvd9'
] = match_to_update.bvd9  # Because parent_name is one-to-one to parent_bvd9

match_to_update.loc[
    match_to_update.is_orbis_parent, 'orbis_parent_country_2DID_iso'
] = match_to_update.parent_country_2DID_iso


match_to_update['is_orbis_sub'] = ~match_to_update['sub_company_name'].isna()

match_to_update.loc[match_to_update.is_orbis_sub, 'orbis_sub_bvd_name'] = match_to_update.sub_company_name

# match_to_update.loc[
#     match_to_update.is_orbis_sub, 'orbis_sub_bvd9'
# ] = match_to_update.sub_bvd9
# ! sub_name is usually NOT one-to-one to sub_bvd9 and often multiple subsidiaries in a same country
# even have similar sub_names

match_to_update.loc[
    match_to_update.is_orbis_sub, 'orbis_sub_country_2DID_iso'
] = match_to_update.sub_country_2DID_iso

# print(list(set(match_update_cols).difference(list(match_to_update.columns))))

# Save step file for archive and next match starter
match_to_update.to_csv(
    output_path,
    columns=['soeur_name'] + match_to_update_cols + new_match_cols,
    index=False,
    float_format='%.10f',
    na_rep='#N/A'
)

match_to_update.to_csv(
    reg.project_path.joinpath(r'ref_tables\soeur_to_orbis_' + company_level + '_table_new.csv'),
    columns=['soeur_name'] + match_to_update_cols,
    index=False,
    float_format='%.10f',
    na_rep='#N/A'
)
# </editor-fold>
