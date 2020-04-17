import pandas as pd


def import_parent_ids_from_orbis_xls(root, file_number, company_type):
    parent_ids = pd.DataFrame()

    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        # Read input list of companies
        df = pd.read_excel(root.joinpath(str(company_type) + ' - identification #' + str(number) + '.xlsx'),
                           sheet_name='Results',
                           names=['rank', 'company_name', 'bvd9', 'bvd_id', 'legal_entity_id', 'country_2DID_iso',
                                  'NACE_4Dcode', 'NACE_desc', 'subs_n',
                                  'guo_type', 'guo_name', 'guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id',
                                  'guo_country_2DID_iso'],
                           na_values='n.a.',
                           dtype={
                               **{col: str for col in ['company_name', 'bvd9', 'bvd_id', 'legal_entity_id',
                                                       'country_2DID_iso', 'NACE_4Dcode', 'NACE_desc',
                                                       'guo_type', 'guo_name', 'guo_bvd9', 'guo_bvd_id',
                                                       'guo_legal_entity_id', 'guo_country_2DID_iso']}
                           }
                           ).drop(columns='rank')

        parent_ids = parent_ids.append(df)

    return parent_ids


def import_parent_fins_from_orbis_xls(root, file_number, oprev_ys, rnd_ys, LY):
    parent_fins = pd.DataFrame()

    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        # Read input list of company financials
        df = pd.read_excel(root.joinpath('parent company - financials #' + str(number) + '.xlsx'),
                           sheet_name='Results',
                           names=['rank', 'company_name', 'bvd9', ]
                                 + ['Emp_number_y' + LY, 'sales_y' + LY]
                                 + rnd_ys[::-1] + oprev_ys[::-1],
                           na_values='n.a.',
                           dtype={
                               **{col: str for col in
                                  ['company_name', 'bvd9']}
                               # **{col: float for col in
                               #    ['operating_revenue_y' + LY, 'sales_y' + LY, 'Emp_number_y' + LY]
                               #    + rnd_ys
                               #    }
                           }
                           ).drop(columns=['rank'])

        parent_fins = parent_fins.append(df)

    return parent_fins


def import_sub_ids_from_orbis_xls(root, file_number):
    sub_ids = pd.DataFrame()

    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        df = pd.read_excel(
            root.joinpath('subsidiary - identification #' + str(number) + '.xlsx'),
            sheet_name='Results',
            na_values=['No data fulfill your filter criteria', 'n.a.'],
            names=['rank', 'company_name', 'bvd9', 'sub_company_name', 'sub_bvd9', 'sub_bvd_id',
                   'sub_legal_entity_id', 'sub_country_2DID_iso', 'sub_NACE_4Dcode', 'sub_NACE_desc', 'sub_lvl'],
            dtype={
                **{col: str for col in
                   ['rank', 'company_name', 'bvd9', 'subsidiary_name', 'sub_bvd9',
                    'sub_bvd_id', 'sub_legal_entity_id', 'sub_country_2DID_iso', 'sub_NACE_4Dcode', 'sub_NACE_desc']}
                # 'sub_lvl': pd.Int8Dtype()
            }
        ).drop(columns=['rank'])

        # Consolidate list of subsidiaries
        sub_ids = sub_ids.append(df)

    return sub_ids


def import_sub_fins_from_orbis_xls(root, file_number, oprev_ys, rnd_ys, LY):
    sub_fins = pd.DataFrame()

    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        df = pd.read_excel(root.joinpath('subsidiary - financials #' + str(number) + '.xlsx'),
            sheet_name='Results',
            names=['rank', 'sub_company_name', 'sub_bvd9'] +
                  ['trade_desc', 'products&services_desc', 'full_overview_desc'] + oprev_ys[::-1] + rnd_ys[::-1],
            na_values='n.a.',
            dtype={
                **{col: str for col in
                   ['sub_company_name', 'sub_bvd9', 'trade_desc', 'products&services_desc', 'full_overview_desc']}
                # **{col: float for col in oprev_ys[::-1] + rnd_ys[::-1]}
            }
        ).drop(columns=['rank'])

        # Consolidate subsidiaries financials
        sub_fins = sub_fins.append(df)

    return sub_fins