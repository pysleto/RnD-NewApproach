import sys

from config import registry as reg

import pandas as pd
import numpy as np

from benchmark import by_methods as by

import seaborn as sns
import matplotlib.pyplot as plt

file_number = 75

account_types = pd.DataFrame()

if not reg.case_path.joinpath('account_types.csv').exists():
    # Read input files
    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        account_part = pd.read_excel(
            reg.case_path.joinpath('input', 'account_types', 'account_types_#' + str(number) + '.xlsx'),
            sheet_name='Results',
            names=['rank',
                   'company_name', 'bvd9', 'quoted', 'conso', 'country_2DID_iso', 'Emp_number_ly'] +
                  ['trade_desc', 'products&services_desc', 'full_overview_desc'] +
                  ['guo_name', 'guo_bvd9'] + reg.rnd_ys[::-1] + reg.oprev_ys[::-1],
            na_values='n.a.',
            dtype={
                **{col: str for col in
                   ['company_name', 'bvd9', 'quoted', 'conso', 'country_2DID_iso', 'trade_desc',
                    'products&services_desc', 'full_overview_desc', 'guo_name', 'guo_bvd9']},
                **{col: float for col in reg.rnd_ys + reg.oprev_ys + ['Emp_number_ly']}
            }
            ).drop(columns=['rank'])

        # Consolidate subsidiaries financials
        account_types = account_types.append(account_part[account_part['conso'].isin(['C*', 'C1', 'C2', 'U1', 'U2', 'LF'])])

    # Sum RnD expenses
    account_types['rnd_sum'] = account_types.loc[:, reg.rnd_ys].sum(axis=1)
    account_types['oprev_sum'] = account_types.loc[:, reg.oprev_ys].sum(axis=1)

    account_types['has_rnd'] = np.where(account_types['rnd_sum'] > 0, True, False)

    # Flag quoted or attached to quoted
    account_types['is_quoted'] = np.where(account_types['quoted'] == 'Yes', True, False)
    account_types['is_sub_of_quoted'] = np.where(~account_types['guo_name'].isna(), True, False)
    account_types['is_in_quoted'] = np.where((account_types['is_quoted']) & (account_types['is_sub_of_quoted']), True,
                                             False)

    # Flag if active in clean energy based on keywords
    for category in reg.categories:

        account_types[category] = False

        for keyword in reg.keywords[category]:
            account_types[category] |= account_types['trade_desc'].str.contains(keyword, case=False, regex=False) | \
                                       account_types['products&services_desc'].str.contains(keyword, case=False,
                                                                                            regex=False) | \
                                       account_types['full_overview_desc'].str.contains(keyword, case=False,
                                                                                        regex=False)

    account_types['is_active_clean_energy'] = list(
        map(bool, account_types[[cat for cat in reg.categories if cat not in ['generation', 'rnd']]].sum(axis=1)))

    account_types['is_potential_rnd_performer'] = account_types['rnd']

    # Merge with country ref and map with world regions
    ref_country = pd.read_csv(reg.project_path.joinpath('ref_tables', 'country_table.csv'))

    account_types = pd.merge(
        account_types,
        ref_country[['country_2DID_iso', 'world_player']],
        left_on='country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    )

    account_types['world_player'].replace('TAX HAVEN', 'ROW', inplace=True)

    # Save output as csv
    print('Save account_types.csv micro data')

    account_types.to_csv(reg.case_path.joinpath('account_types.csv'),
                         columns=['company_name', 'bvd9', 'conso', 'country_2DID_iso', 'world_player', 'Emp_number_ly',
                                  'is_top', 'is_in_quoted', 'is_quoted', 'is_sub_of_quoted', 'has_rnd',
                                  'is_active_clean_energy', 'is_potential_rnd_performer', 'rnd_sum', 'oprev_sum'] +
                                  reg.rnd_ys[::-1] + reg.oprev_ys[::-1],
                         float_format='%.10f',
                         index=False,
                         na_rep='#N/A'
                         )

print('Read account_types.csv micro data')

account_types = pd.read_csv(reg.case_path.joinpath('account_types.csv'),
                            na_values='#N/A',
                            dtype={
                                **{col: str for col in
                                   ['company_name', 'bvd9', 'conso', 'country_2DID_iso', 'world_player']},
                                **{col: float for col in ['Emp_number_ly', 'rnd_sum', 'oprev_sum']},
                                **{col: bool for col in
                                   ['is_top', 'is_in_quoted', 'is_quoted', 'is_sub_of_quoted', 'has_rnd',
                                    'is_active_clean_energy', 'is_potential_rnd_performer']}
                            }
                            )

print('Check duplicates')

account_types.dropna(subset=['bvd9', 'oprev_sum'], inplace=True)
account_types.sort_values('oprev_sum', ascending=False).drop_duplicates(subset=['bvd9', 'conso'], keep='first', inplace=True)

# Group duplicates by conso type combination for analysis
account_types = by.check_duplicates(account_types)

account_pairs_grouped = by.count_conso_pairs(account_types)

# account_types[account_types['is_C1LF'] == True].to_csv(
#     reg.case_path.joinpath('account_dup_init.csv'),
#     float_format='%.10f',
#     index=False,
#     na_rep='#N/A'
# )
#
# account_pairs_grouped.to_csv(reg.case_path.joinpath('account_pairs_init.csv'),
#                              float_format='%.10f',
#                              na_rep='#N/A'
#                              )

for conso_pair in ['C*C1', 'C*C2', 'C1C2', 'C2LF',  'U1LF', 'C1LF', 'C*LF']:
    account_types.drop_duplicates(subset=['bvd9', 'is_' + conso_pair], keep='first', inplace=True)

account_types = account_types[
    (account_types['is_C*U1'] == False) &
    (account_types['is_C1U1'] == False) &
    (account_types['is_C2U1'] == False) &
    (account_types['conso'] != 'U2')
]

account_types = by.check_duplicates(account_types)

# account_types[account_types['is_C*C1'] == True].to_csv(
#     reg.case_path.joinpath('account_dup_final.csv'),
#     float_format='%.10f',
#     index=False,
#     na_rep='#N/A'
# )

# account_pairs_grouped = by.count_conso_pairs(account_types)
#
# account_pairs_grouped.to_csv(reg.case_path.joinpath('account_pairs_final.csv'),
#                              float_format='%.10f',
#                              na_rep='#N/A'
#                              )

sample_size = account_types['bvd9'].nunique()

print('Number of distinct companies in sample: ' + str(sample_size))

grouped_rnd = account_types[
    (account_types['is_quoted'] == True) &
    (account_types['conso'].isin(['C*', 'C1', 'C2']))
    ].groupby('bvd9').sum()

top_conso_rnd = grouped_rnd.nlargest(2000, ['rnd_sum'])

account_types['is_top'] = account_types['bvd9'].isin(top_conso_rnd.index)

print('Normalize')

# norm = (account_types['world_player'] == 'USA') & (account_types['conso'].isin(['C1']))
#
account_types['rnd_norm'] = account_types['rnd_sum'] / account_types['rnd_sum'].sum()
account_types['oprev_norm'] = account_types['oprev_sum'] / account_types['oprev_sum'].sum()

print('Define categories and labels')

account_types['type'] = np.where(account_types['conso'].isin(['C*', 'C1', 'C2']), 'Consolidated', 'Unconsolidated')

account_types.loc[(account_types['type'] == 'Consolidated') &
    (account_types['has_rnd'] == True) & (account_types['is_top'] == True), 'label'
] = 'Top R&D investors'
account_types.loc[(account_types['type'] == 'Consolidated') &
    (account_types['has_rnd'] == True) & (account_types['is_top'] == False), 'label'
] = 'Other R&D investors'
account_types.loc[(account_types['type'] == 'Consolidated') &
    (account_types['has_rnd'] == False), 'label'
] = 'No disclosed R&D investments or activity'

account_types.loc[(account_types['type'] == 'Unconsolidated') &
                  (account_types['has_rnd'] == True),
                  'label'
] = 'Other R&D investors'
account_types.loc[(account_types['type'] == 'Unconsolidated') &
                  (account_types['has_rnd'] == False) &
                  (account_types['is_potential_rnd_performer'] == True),
                  'label'
] = 'Potential R&D performer'
account_types.loc[(account_types['type'] == 'Unconsolidated') &
                  (account_types['has_rnd'] == False) &
                  (account_types['is_potential_rnd_performer'] == False),
                  'label'
] = 'No disclosed R&D investments or activity'

print('Group by categories')

output = account_types.groupby(
    ['conso', 'world_player', 'is_top', 'has_rnd', 'is_active_clean_energy', 'is_potential_rnd_performer', 'type', 'label']
).agg(
    {'oprev_sum': 'sum', 'rnd_sum': 'sum', 'oprev_norm': 'sum', 'rnd_norm': 'sum', 'bvd9': 'count'}
)

print('Save grouped data')

print(list(output.columns))

output.to_csv(reg.case_path.joinpath('account_types_grouped.csv'),
              float_format='%.10f',
              index=True,
              na_rep='#N/A'
              )
