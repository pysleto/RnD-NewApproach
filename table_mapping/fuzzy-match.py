# Import libraries
import sys
import os
from pathlib import Path

import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from tabulate import tabulate

from data_input import file_loader as load
from mapping import input

# Set  dataframe display options
pd.options.display.max_columns = None
pd.options.display.width = None

root = Path(r'C:\Users\Simon\PycharmProjects\rnd-private')

# TODO: apply to ORBIS suvbs

# ORBIS parents
parent_ids = pd.read_csv(
    root.joinpath(r'cases/2018_GLOBAL/1 - identification - parents.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['bvd9', 'guo_bvd9' 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
    }
)

# jrc004_mnc_ids = load.jrc004_mnc_from_xls(root, r'data_input/SOEUR_RnD/PATSTAT_2019b/Input_Data/004_MNCs/JRC004_MNC.xlsx')
# ref_ids = input.load_mnc_table(root, r'table_mapping\MNC_table.xlsx')

mnc_table_cols = [
    'scoreboard_company_name',
    'scoreboard_country_2DID',
    'scoreboard_company_industry',
    'open_refine_Company_name',
    'soeur_group_id',
    'soeur_group_name',
    'soeur_group_country_2DID',
    'FP_bvd_company_name',
    'FP_bvd_id',
    'scoreboard_ICB_A',
    'scoreboard_ICB_B',
    'scoreboard_ICB_id',
    'scoreboard_ICB_3_5Dcode',
    'scoreboard_ICB_3_name',
    'scoreboard_ICB_D',
    'FP_bvd_NACE_4Dcode_FP'
]

mnc_table = pd.read_csv(
    root.joinpath(r'table_mapping\MNC_table.csv'),
    names=mnc_table_cols,
    na_values='#N/A',
    dtype=str
)

ref_ids = mnc_table[['soeur_group_name']]

i_count = 0
# i_match = 0

ref_ids.set_index('soeur_group_name', inplace=True)

# print(ref_ids)

mnc_table.set_index('soeur_group_name', inplace=True)

# print(mnc_table)

to_search_on = ref_ids.index.value_counts().sum()

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

    ratio_match = process.extractOne(
        name_to_match, parent_ids['company_name'], scorer=fuzz.ratio
    )

    print('... fuzz.partial_ratio')

    partial_ratio_match = process.extractOne(
        name_to_match, parent_ids['company_name'], scorer=fuzz.partial_ratio
    )

    print('... fuzz.token_sort_ratio')

    token_sort_ratio_match = process.extractOne(
        name_to_match, parent_ids['company_name'], scorer=fuzz.token_sort_ratio
    )

    print('... fuzz.token_set_ratio')

    token_set_ratio_match = process.extractOne(
        name_to_match, parent_ids['company_name'], scorer=fuzz.token_set_ratio
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

    # if match[1] == 100:
    #     # output[scoreboard_name] = {'matched_company_name': matched_company_name, 'rate': rate, 'method': 'fuzz.ratio'}
    #     # jrc004_mnc_ids.loc[scoreboard_name, 'bvd_parent_company_name_SL'] = match[0]
    #
    #     i_match += 1
    #     is_match = True

    i_count += 1

output = pd.merge(
    mnc_table,
    ref_ids,
    left_index=True, right_index=True,
    how='left',
    suffixes=(False, False)
)

output.reset_index(inplace=True)

output_cols = [
    'scoreboard_company_name',
    'scoreboard_country_2DID',
    'open_refine_Company_name',
    'soeur_group_id',
    'soeur_group_name',
    'FP_bvd_company_name',
    'FP_bvd_id',
    'ratio_name',
    'ratio_rate',
    'partial_ratio_name',
    'partial_ratio_rate',
    'token_sort_ratio_name',
    'token_sort_ratio_rate',
    'token_set_ratio_name',
    'token_set_ratio_rate'
]

output.to_csv(
    root.joinpath(r'table_mapping\match_output.csv'),
    columns=output_cols,
    float_format='%.10f',
    na_rep='#N/A')
