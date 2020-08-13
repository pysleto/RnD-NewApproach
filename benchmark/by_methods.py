from config import registry as reg

import pandas as pd
import numpy as np

from tabulate import tabulate

conso_pairs = ['C*C1', 'C*C2', 'C1C2', 'C*U2', 'C1U2', 'C2U2', 'U1U2', 'U2LF', 'C2LF', 'U1LF', 'C1LF', 'C*LF',
               'C*U1', 'C1U1', 'C2U1'
               ]

def check_duplicates(account_types):

    # 'C*C1', 'C*C2', 'C1C2', 'C2LF',  'U1LF', 'C1LF', 'C*LF' > To screen on highest turnover
    # 'C*U2', 'C1U2', 'C2U2', 'U1U2', 'U2LF' > N/A since U2 and LF excluded
    #
    # 'C*U1', 'C1U1', 'C2U1'

    # Flags all observed conso types for a given bvd9
    for conso in ['C*', 'C1', 'C2', 'U1', 'U2', 'LF']:
        account_types['is_' + conso] = np.where(
            account_types['bvd9'].isin(account_types.loc[account_types['conso'] == conso, 'bvd9']
                                       ), True, False)

    for conso_pair in conso_pairs:
        account_types['is_' + conso_pair] = np.where(
            (account_types['is_' + conso_pair[:2]] == True) &
            (account_types['is_' + conso_pair[-2:]] == True),
            True, False
        )

    account_types.sort_values('oprev_sum', ascending=False)

    return account_types


def count_conso_pairs(account_types):
    account_pairs_grouped = pd.DataFrame()

    for conso_pair in conso_pairs:
        account_pairs_grouped.loc[conso_pair, 'Count'] = account_types['is_' + conso_pair].sum()

    return account_pairs_grouped


def select_by_account(
        data,
        type_label
):
    conso_type = str(type_label) + '_conso'
    id_type = {'guo': 'guo_bvd9', 'parent': 'bvd9', 'sub': 'sub_bvd9'}
    new_data = data.copy()
    output = pd.DataFrame()

    # Drop #N/A
    new_data.dropna(subset=[id_type[type_label]], inplace=True)
    # new_data.drop(new_data[id_type[type_label]] == '0', inplace=True)

    # Prioritize consolidated only accounts
    new_data = new_data[new_data[conso_type] == 'C1'].copy()
    current_ids = new_data[id_type[type_label]]
    output = output.append(
        pd.DataFrame(data={'#ids': str(new_data[id_type[type_label]].count())}, index=['C1'])
    )

    for conso_label in ['C2', 'C*', 'U1', 'U*', 'LF', 'NF']:
        new_data = new_data.append(
            data[
                (data[conso_type] == conso_label) &
                (data[id_type[type_label]].isin(current_ids) == False)
                ].copy()
        )
        current_ids = new_data[id_type[type_label]]

        output = output.append(
            pd.DataFrame(data={'#ids': str(new_data[id_type[type_label]].count())}, index=[conso_label])
        )

    # Report
    output = output.transpose()
    print(tabulate(output, tablefmt='simple', headers=output.columns, floatfmt='10,.0f'))

    return new_data
