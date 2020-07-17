from config import registry as reg

import pandas as pd
import numpy as np

import seaborn as sns
import matplotlib.pyplot as plt

file_number = 1

account_types = pd.DataFrame()

if not reg.case_path.joinpath('account_types.csv').exists():
    # Read input files
    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        account_part = pd.read_excel(reg.case_path.joinpath('input', 'account_types#' + str(number) + '.xlsx'),
                           sheet_name='Results',
                           names=['rank',
                                  'company_name', 'bvd9', 'quoted', 'conso', 'country_2DID_iso', 'Emp_number_ly'] +
                                 ['trade_desc', 'products&services_desc', 'full_overview_desc'] +
                                 ['guo_name', 'guo_bvd9'] + reg.rnd_ys[::-1],
                           na_values='n.a.',
                           dtype={
                               **{col: str for col in
                                  ['company_name', 'bvd9', 'quoted', 'conso', 'country_2DID_iso', 'trade_desc',
                                   'products&services_desc', 'full_overview_desc', 'guo_name', 'guo_bvd9']},
                               **{col: float for col in reg.rnd_ys + ['Emp_number_ly']}
                           }
                           ).drop(columns=['rank'])

        # Consolidate subsidiaries financials
        account_types = account_types.append(account_part)

    # Sum RnD expenses
    account_types['rnd_sum'] = account_types.loc[:, reg.rnd_ys].sum(axis=1)

    # account_types['is_top'] = account_types[account_types.isin(account_types.nlargest(10, ['rnd_sum']))]

    # Flag quoted or attached to quoted
    account_types['is_quoted'] = np.where(account_types['quoted'] == 'Yes', 'True', 'False')
    account_types['is_sub_of_quoted'] = np.where(~account_types['guo_name'].isna(), 'True', 'False')

    # Flag if active in clean energy based on keywords
    for category in reg.categories:

        account_types[category] = False

        for keyword in reg.keywords[category]:
            account_types[category] |= account_types['trade_desc'].str.contains(keyword, case=False, regex=False) | \
                                  account_types['products&services_desc'].str.contains(keyword, case=False, regex=False) | \
                                  account_types['full_overview_desc'].str.contains(keyword, case=False, regex=False)

    account_types['is_active_clean_energy'] = list(
            map(bool, account_types[[cat for cat in reg.categories if cat not in ['generation', 'rnd']]].sum(axis=1)))

    # Merge with country ref and map with world regions
    ref_country = pd.read_csv(reg.project_path.joinpath('ref_tables', 'country_table.csv'))

    account_types = pd.merge(
        account_types,
        ref_country[['country_2DID_iso', 'world_player']],
        left_on='country_2DID_iso', right_on='country_2DID_iso',
        how='left',
        suffixes=(False, False)
    )

    # Save output as csv
    account_types.to_csv(reg.case_path.joinpath('account_types.csv'),
                         columns=['company_name', 'bvd9', 'conso', 'country_2DID_iso', 'world_player', 'Emp_number_ly',
                                  'is_top', 'is_quoted', 'is_sub_of_quoted', 'is_active_clean_energy', 'rnd_sum'],
                         float_format='%.10f',
                         index=False,
                         na_rep='#N/A'
                         )

account_types = pd.read_csv(reg.case_path.joinpath('account_types.csv'),
                            na_values='#N/A',
                            dtype={
                                **{col: str for col in
                                   ['company_name', 'bvd9', 'conso', 'country_2DID_iso', 'world_player']},
                                **{col: float for col in ['Emp_number_ly', 'rnd_sum']},
                                **{col: bool for col in
                                   ['is_top', 'is_quoted', 'is_sub_of_quoted', 'is_active_clean_energy']}
                            }
                            )
