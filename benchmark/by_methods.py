from config import registry as reg

import pandas as pd
import numpy as np

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

