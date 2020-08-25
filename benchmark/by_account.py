import sys

from config import registry as reg

import pandas as pd
import numpy as np

from benchmark import by_methods as by_mtd
from config import col_ids as col

import seaborn as sns
import matplotlib.pyplot as plt

file_number = 396

account_types = pd.DataFrame()

if not reg.case_path.joinpath('account_types.csv').exists():
    # Read input files
    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        account_part = pd.read_excel(
            reg.case_path.joinpath('input', 'account_types_over2m', 'account_types_#' + str(number) + '.xlsx'),
            sheet_name='Results',
            names=['rank',
                   'company_name', 'bvd9', 'quoted', 'parent_conso', 'country_2DID_iso', 'Emp_number_ly'] +
                  ['trade_desc', 'products&services_desc', 'full_overview_desc'] +
                  ['guo_name', 'guo_bvd9'] + reg.rnd_ys[::-1] + reg.oprev_ys[::-1],
            na_values='n.a.',
            dtype={
                **{col: str for col in
                   ['company_name', 'bvd9', 'quoted', 'parent_conso', 'country_2DID_iso', 'trade_desc',
                    'products&services_desc', 'full_overview_desc', 'guo_name', 'guo_bvd9']},
                **{col: float for col in reg.rnd_ys + reg.oprev_ys + ['Emp_number_ly']}
            }
        ).drop(columns=['rank'])

        # Consolidate subsidiaries financials
        account_types = account_types.append(
            account_part[account_part['parent_conso'].isin(['C1', 'C2', 'C*', 'U1', 'U2', 'U*', 'LF', 'NF'])])

    print('Financials processing')

    # Sum RnD expenses
    account_types['rnd_sum'] = account_types.loc[:, reg.rnd_ys].sum(axis=1)
    account_types['oprev_sum'] = account_types.loc[:, reg.oprev_ys].sum(axis=1)

    account_types['has_rnd'] = np.where(account_types['rnd_sum'] > 0, True, False)

    # Flag quoted or attached to quoted
    account_types['is_quoted'] = np.where(account_types['quoted'] == 'Yes', True, False)
    # account_types['is_sub_of_quoted'] = np.where(~account_types['guo_name'].isna(), True, False)
    # account_types['is_in_quoted'] = np.where((account_types['is_quoted']) & (account_types['is_sub_of_quoted']), True,
    #                                          False)

    print('Keyword match')

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

    print('World region mapping')

    th_realoc_path = r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Data\2019a_ORBIS_LC+HCO50_AA\input\Tax_haven_by_activity_main_location.csv'

    main_eco_activity_2DID_ISO = pd.read_csv(
        th_realoc_path,
        error_bad_lines=False, encoding='UTF-8',
        dtype=str
    )

    account_types = pd.merge(
        account_types,
        main_eco_activity_2DID_ISO[['bvd9', 'main_eco_location_2DID_ISO']],
        left_on='bvd9', right_on='bvd9',
        how='left',
        suffixes=(False, False)
    )

    account_types['country_2DID_iso'] = np.where(account_types['main_eco_location_2DID_ISO'].isna(),
                                                 account_types['country_2DID_iso'],
                                                 account_types['main_eco_location_2DID_ISO'])

    # Merge with country ref and map with world regions
    ref_country = pd.read_csv(reg.project_path.joinpath('ref_tables', 'country_table.csv'))

    account_types = pd.merge(
        account_types,
        ref_country[['country_2DID_iso', 'world_player']],
        left_on='country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    )

    # TODO: Check economical activity location of companies that are in TAX HAVEN
    # tax_haven_companies = account_types[
    #     account_types['world_player'] == 'TAX HAVEN'
    #     ].groupby(['company_name', 'bvd9', 'parent_conso', 'is_quoted', 'has_rnd',
    #               'is_active_clean_energy']).sum()
    #
    # tax_haven_companies.to_csv(reg.case_path.joinpath('tax_haven_companies.csv'),
    #                            float_format='%.10f',
    #                            na_rep='#N/A'
    #                            )

    account_types['world_player'].replace('TAX HAVEN', 'ROW', inplace=True)

    # Save output as csv
    print('Save account_types.csv micro data')

    account_types.to_csv(reg.case_path.joinpath('account_types.csv'),
                         columns=['company_name', 'bvd9', 'parent_conso', 'country_2DID_iso', 'world_player',
                                  'guo_bvd9', 'Emp_number_ly',
                                  'is_quoted', 'rnd', 'has_rnd', 'is_active_clean_energy',
                                  'rnd_sum', 'oprev_sum'],
                         # + reg.rnd_ys[::-1] + reg.oprev_ys[::-1],
                         float_format='%.10f',
                         index=False,
                         na_rep='#N/A'
                         )

print('Read account_types.csv micro data')

account_types = pd.read_csv(reg.case_path.joinpath('account_types.csv'),
                            na_values='#N/A',
                            dtype={
                                **{col: str for col in
                                   ['company_name', 'bvd9', 'guo_bvd9', 'parent_conso', 'country_2DID_iso', 'world_player']},
                                **{col: float for col in ['Emp_number_ly', 'rnd_sum', 'oprev_sum']},
                                **{col: bool for col in
                                   ['is_quoted', 'rnd', 'has_rnd',
                                    'is_active_clean_energy']}
                            }
                            )

print('Soeur name fuzzy match')

ref_soeur_sub_path = reg.project_path.joinpath(
    'ref_tables/SOEUR_RnD/SOEUR_RnD_2019b/SOEUR_RnD_2019b_20200309_by_sub.csv')

soeur_sub = pd.read_csv(
    ref_soeur_sub_path,
    error_bad_lines=False, encoding='UTF-8',
    dtype=str
)

ref_soeur_group_path = reg.project_path.joinpath('ref_tables/SOEUR_RnD/SOEUR_RnD_2019b/SOEUR_RnD_2019b_20200309_by_group.csv')

soeur_group = pd.read_csv(
    ref_soeur_group_path,
    error_bad_lines=False, encoding='UTF-8',
    dtype=str
)

potential_performer = account_types.loc[
    (account_types['company_name'].isin(soeur_sub['soeur_sub_name'])) |
    (account_types['company_name'].isin(soeur_group['soeur_group_name'])) |
    (account_types['company_name'].isin(soeur_group['orbis_parent_bvd_name'])) |
    (account_types['bvd9'].isin(soeur_group['orbis_parent_bvd9']))
    , 'bvd9']

potential_performer.drop_duplicates(inplace=True)

# Flag if identified in soeur as a parent or a sub

account_types['is_potential_performer'] = account_types['bvd9'].isin(potential_performer)

print('Check duplicates')

account_types.dropna(subset=['bvd9', 'oprev_sum'], inplace=True)
account_types.sort_values('oprev_sum', ascending=False).drop_duplicates(subset=['bvd9', 'parent_conso'], keep='first',
                                                                        inplace=True)

account_types = by_mtd.select_by_account(account_types, 'parent')

print('Normalize')

# norm = (account_types['world_player'] == 'USA') & (account_types['conso'].isin(['C1']))
#
account_types['rnd_norm'] = account_types['rnd_sum'] / account_types['rnd_sum'].sum()
account_types['oprev_norm'] = account_types['oprev_sum'] / account_types['oprev_sum'].sum()

print('Flag guos, parents and subs with new approach R&D')

new_app_sub_rnd = pd.read_csv(
    reg.sub_rnd_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('Define consolidation type')

account_types['type'] = np.where(account_types['parent_conso'].isin(['C*', 'C1', 'C2']), 'Consolidated',
                                 'Unconsolidated')

print('Define activity labels')

account_types['label'] = np.where(account_types['is_active_clean_energy'] == True,
                                  'Active in clean energy but with no indication of R&D activities',
                                  'Not identified as active in clean energy')

account_types.loc[((account_types['is_active_clean_energy'] == True) & (account_types['has_rnd'] == True)
                   ), 'label'
] = 'Clean energy R&D performers (new approach)'
account_types.loc[((account_types['is_active_clean_energy'] == True) &
                   (account_types['is_potential_performer'] == True) | (account_types['rnd'] == True)),
                  'label'
] = 'Other likely clean energy R&D performers (not captured via reporting parents)'
account_types.loc[(
                          (account_types['is_active_clean_energy'] == True) &
                          (account_types['has_rnd'] == True)
                  ), 'label'
] = 'Clean energy R&D performers (new approach)'
account_types.loc[((account_types['is_active_clean_energy'] == False) &
                   (account_types['is_potential_performer'] == True)),
                  'label'
] = 'Other likely clean energy R&D performers (not captured via keywords)'

account_types.loc[((account_types['bvd9'].isin(new_app_sub_rnd['sub_bvd9'])) |
                   (account_types['bvd9'].isin(new_app_sub_rnd['bvd9']))
                   # (account_types['guo_bvd9'].isin(new_app_sub_rnd['guo_bvd9']))
                   ), 'label'
] = 'Clean energy R&D performers (new approach)'


# output_screened = account_types.loc[
#     (account_types['label'] != 'Not active in clean energy') &
#     (account_types['parent_conso'] != 'U2'),
#     ['company_name', 'bvd9', 'parent_conso', 'country_2DID_iso', 'world_player', 'guo_bvd9', 'is_quoted', 'rnd',
#      'has_rnd', 'is_active_clean_energy', 'rnd_sum', 'oprev_sum', 'is_potential_performer', 'rnd_norm',
#      'oprev_norm', 'type', 'label']
# ]

print('Group by categories')

output_grouped = account_types.groupby(
    ['parent_conso', 'world_player', 'rnd', 'has_rnd', 'is_active_clean_energy', 'is_potential_performer',
     'type', 'label']
).agg(
    {'oprev_sum': 'sum', 'rnd_sum': 'sum', 'oprev_norm': 'sum', 'rnd_norm': 'sum', 'bvd9': 'count'}
)

print('Save grouped data')

# output_screened.to_csv(reg.case_path.joinpath('account_types_screened.csv'),
#                        float_format='%.10f',
#                        index=False,
#                        na_rep='#N/A'
#                        )

output_grouped.to_csv(reg.case_path.joinpath('account_types_grouped.csv'),
                      float_format='%.10f',
                      index=True,
                      na_rep='#N/A'
                      )
