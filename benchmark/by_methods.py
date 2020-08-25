from config import registry as reg

import pandas as pd
import numpy as np

from tabulate import tabulate


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

    for conso_label in ['C2', 'C*', 'U1', 'U2', 'U*', 'LF', 'NF']:
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
