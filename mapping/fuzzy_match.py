# Import libraries
import sys
import os
from pathlib import Path

import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from tabulate import tabulate

from data_input import file_loader as load

# Set  dataframe display options
pd.options.display.max_columns = None
pd.options.display.width = None

root = Path(r'C:\Users\Simon\PycharmProjects\rnd-private')


# <editor-fold desc="#01 - Load current mapping">

# jrc004_mnc_ids = load.jrc004_mnc_from_xls(root, r'data_input/SOEUR_RnD/PATSTAT_2019b/Input_Data/004_MNCs/JRC004_MNC.xlsx')

# TODO: Update from consolidated reference table
# current_map_cols = [
#     'soeur_name',
#     'is_soeur_group',
#     'soeur_group_id',
#     'is_soeur_sub',
#     'soeur_sub_id',
#     'scoreboard_name',
#     'current_bvd_name',
#     'current_bvd_id',
#     'soeur_parent_id',
#     'soeur_parent_name'
# ]

current_map = pd.read_csv(
    root.joinpath(r'mapping/output/current_soeur_match_2020-05-02.csv'),
    na_values='#N/A',
    dtype=str
)

ref_ids = pd.DataFrame(current_map.loc[~current_map.current_bvd_name.isna(), 'current_bvd_name'])
# ref_ids = current_map[['soeur_group_name']]

i_count = 0
# i_match = 0

ref_ids.set_index('current_bvd_name', inplace=True)
# ref_ids.set_index('soeur_group_name', inplace=True)

# print(ref_ids)

current_map.set_index('current_bvd_name', inplace=True)
# current_map.set_index('soeur_group_name', inplace=True)

# print(mnc_table)

to_search_on = ref_ids.index.value_counts().sum()
# </editor-fold>


# <editor-fold desc="#02 - Load new company names and ids to map with">

# TODO: apply to ORBIS subs
# ORBIS parents
parent_ids = pd.read_csv(
    root.joinpath(r'cases/2018_GLOBAL/1 - identification - parents.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['bvd9', 'guo_bvd9' 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
    }
)
# </editor-fold>

for name_to_match in ref_ids.index.values[1:]:

    # name_to_match = name_to_match.encode(encoding="utf-8", errors="ignore")

    is_match = False

    print(
        tabulate([[
            # str(i_match) + ' matches',
            str(i_count) + ' / ' + str(to_search_on) + ' steps',
            # 'Is a match: ' + str(is_match),
            'Input: ' + str(name_to_match),
            # 'Outputs:' + match[0],
            # '(' + str(match[1]) + ')'
        ]], tablefmt="plain")
    )

    print('... fuzz.ratio')

    current_ratio_match = process.extractOne(
        name_to_match, parent_ids['company_name'], scorer=fuzz.ratio
    )

    # print('... fuzz.ratio')
    #
    # ratio_match = process.extractOne(
    #     name_to_match, parent_ids['company_name'], scorer=fuzz.ratio
    # )
    #
    # print('... fuzz.partial_ratio')
    #
    # partial_ratio_match = process.extractOne(
    #     name_to_match, parent_ids['company_name'], scorer=fuzz.partial_ratio
    # )
    #
    # print('... fuzz.token_sort_ratio')
    #
    # token_sort_ratio_match = process.extractOne(
    #     name_to_match, parent_ids['company_name'], scorer=fuzz.token_sort_ratio
    # )
    #
    # print('... fuzz.token_set_ratio')
    #
    # token_set_ratio_match = process.extractOne(
    #     name_to_match, parent_ids['company_name'], scorer=fuzz.token_set_ratio
    # )

    # fuzz.ratio
    # fuzz.partial_ratio
    # fuzz.token_sort_ratio
    # fuzz.token_set_ratio

    ref_ids.loc[name_to_match, 'current_ratio_name'] = current_ratio_match[0]
    ref_ids.loc[name_to_match, 'current_ratio_rate'] = current_ratio_match[1]
    # ref_ids.loc[name_to_match, 'ratio_name'] = ratio_match[0]
    # ref_ids.loc[name_to_match, 'ratio_rate'] = ratio_match[1]
    # ref_ids.loc[name_to_match, 'partial_ratio_name'] = partial_ratio_match[0]
    # ref_ids.loc[name_to_match, 'partial_ratio_rate'] = partial_ratio_match[1]
    # ref_ids.loc[name_to_match, 'token_sort_ratio_name'] = token_sort_ratio_match[0]
    # ref_ids.loc[name_to_match, 'token_sort_ratio_rate'] = token_sort_ratio_match[1]
    # ref_ids.loc[name_to_match, 'token_set_ratio_name'] = token_set_ratio_match[0]
    # ref_ids.loc[name_to_match, 'token_set_ratio_rate'] = token_set_ratio_match[1]

    # if match[1] == 100:
    #     # output[scoreboard_name] = {'matched_company_name': matched_company_name, 'rate': rate, 'method': 'fuzz.ratio'}
    #     # jrc004_mnc_ids.loc[scoreboard_name, 'bvd_parent_company_name_SL'] = match[0]
    #
    #     i_match += 1
    #     is_match = True

    ref_ids.to_csv(
        root.joinpath(r'mapping\current_match.csv'),
        float_format='%.10f',
        na_rep='#N/A'
    )

    i_count += 1

output = pd.merge(
    current_map,
    ref_ids,
    left_index=True, right_index=True,
    how='left',
    suffixes=(False, False)
)

output.reset_index(inplace=True)

# output_cols = [
#     'scoreboard_company_name',
#     'scoreboard_country_2DID',
#     'open_refine_Company_name',
#     'soeur_group_id',
#     'soeur_group_name',
#     'FP_bvd_company_name',
#     'FP_bvd_id',
#     'ratio_name',
#     'ratio_rate',
#     'partial_ratio_name',
#     'partial_ratio_rate',
#     'token_sort_ratio_name',
#     'token_sort_ratio_rate',
#     'token_set_ratio_name',
#     'token_set_ratio_rate'
# ]

# TODO: Keep original soeur_bvd_ids_FP for reference
output_cols = [
    'soeur_name',
    'is_soeur_group', 'soeur_group_id',
    'is_soeur_sub', 'soeur_sub_id',
    'soeur_parent_id', 'soeur_parent_name',
    'new_bvd_name', 'step', 'comment',
    'scoreboard_name',
    'current_bvd_name', 'current_bvd_id', 'current_ratio_name', 'current_ratio_rate',
    'ratio_name', 'ratio_rate',
    'partial_ratio_name', 'partial_ratio_rate',
    'token_sort_ratio_name', 'token_sort_ratio_rate',
    'token_set_ratio_name', 'token_set_ratio_rate'
]

# TODO: Check output format of every columna (e.g. bool cannot have NA values)
output.to_csv(
    root.joinpath(r'mapping\match_output.csv'),
    columns=output_cols,
    index=False,
    float_format='%.10f',
    na_rep='#N/A')
