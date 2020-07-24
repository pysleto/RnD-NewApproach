import pandas as pd


def parent_ids_from_orbis_xls(root,
                              file_number,
                              company_type):
    parent_ids = pd.DataFrame()

    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        # Read input list of companies
        df = pd.read_excel(root.joinpath(str(company_type) + '_parent_ids_#' + str(number) + '.xlsx'),
                           sheet_name='Results',
                           names=['rank', 'company_name', 'bvd9', 'parent_conso', 'bvd_id', 'legal_entity_id',
                                  'country_2DID_iso', 'NACE_4Dcode', 'NACE_desc', 'subs_n',
                                  'guo_type', 'guo_name', 'guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id',
                                  'guo_country_2DID_iso'],
                           na_values='n.a.',
                           dtype={
                               **{col: str for col in ['company_name', 'bvd9', 'parent_conso', 'bvd_id', 'legal_entity_id',
                                                       'country_2DID_iso', 'NACE_4Dcode', 'NACE_desc',
                                                       'guo_type', 'guo_name', 'guo_bvd9', 'guo_bvd_id',
                                                       'guo_legal_entity_id', 'guo_country_2DID_iso']}
                           }
                           ).drop(columns='rank')

        parent_ids = parent_ids.append(df)

    return parent_ids


def parent_fins_from_orbis_xls(root,
                               file_number,
                               oprev_ys,
                               rnd_ys,
                               ly):
    parent_fins = pd.DataFrame()

    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        # Read input list of company financials
        df = pd.read_excel(root.joinpath('parent_fins_#' + str(number) + '.xlsx'),
                           sheet_name='Results',
                           names=['rank', 'company_name', 'bvd9', 'parent_conso']
                                 + ['Emp_number_y' + ly, 'sales_y' + ly]
                                 + rnd_ys[::-1] + oprev_ys[::-1],
                           na_values='n.a.',
                           dtype={
                               **{col: str for col in
                                  ['company_name', 'bvd9', 'parent_conso']}
                               # **{col: float for col in
                               #    ['operating_revenue_y' + ly, 'sales_y' + ly, 'Emp_number_y' + ly]
                               #    + rnd_ys
                               #    }
                           }
                           ).drop(columns=['rank'])

        parent_fins = parent_fins.append(df)

    return parent_fins


def sub_ids_from_orbis_xls(root,
                           file_number):
    sub_ids = pd.DataFrame()

    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        df = pd.read_excel(
            root.joinpath('sub_ids_#' + str(number) + '.xlsx'),
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


def sub_fins_from_orbis_xls(root,
                            file_number,
                            oprev_ys,
                            rnd_ys
                            ):
    sub_fins = pd.DataFrame()

    for number in list(range(1, file_number + 1)):
        print('File #' + str(number) + '/' + str(file_number))
        df = pd.read_excel(root.joinpath('sub_fins_#' + str(number) + '.xlsx'),
                           sheet_name='Results',
                           names=['rank', 'sub_company_name', 'sub_bvd9', 'sub_conso'] +
                                 ['trade_desc', 'products&services_desc', 'full_overview_desc'] + oprev_ys[
                                                                                                  ::-1] + rnd_ys[::-1],
                           na_values='n.a.',
                           dtype={
                               **{col: str for col in
                                  ['sub_company_name', 'sub_bvd9', 'sub_conso', 'trade_desc', 'products&services_desc',
                                   'full_overview_desc']}
                               # **{col: float for col in oprev_ys[::-1] + rnd_ys[::-1]}
                           }
                           ).drop(columns=['rank'])

        # Consolidate subsidiaries financials
        sub_fins = sub_fins.append(df)

    return sub_fins


def soeur_rnd_from_xls(file_path):
    names_2019b = ['Group_Id', 'Group_Name', 'Group_Country', 'Id_Group_Region', 'Group_Region',
                   'Group_MI_member', 'BvD_ID', 'ICB_ID', 'ICB_3_name', 'Nace_ID', 'Sector_UC',
                   'Group_UC', 'RnD_Group_UC', 'Group_Size', 'Group_R&D_MEUR', 'Group_Sales_MEUR',
                   'Group_Employees', 'Group_Invention', 'Group_Energy_Invention', 'YEAR', 'JRC_Id',
                   'NAME', 'Sector', 'id_world_player', 'World_player', 'MI_member', 'Country_Order',
                   'Country', 'NUTS1', 'NUTS2', 'NUTS3', 'Total_Invention', 'Id_Tech', 'Technology',
                   'Actions', 'Energy_Union_Priority', 'Tech_UC', 'Invention', 'Invention_Granted',
                   'Invention_High_Value', 'Invention_Citation', 'RnD_MEUR', 'Equation']

    # names_2019a = ['Group_Id', 'Group_Name', 'Group_Country', 'Group_Region',
    #                'Group_MI_member', 'BvD_ID', 'ICB_ID', 'ICB_3_name', 'Nace_ID', 'Sector_UC',
    #                'Group_UC', 'RnD_Group_UC', 'Group_Size', 'Group_R&D_MEUR', 'Group_Sales_MEUR',
    #                'Group_Employees', 'Group_Invention', 'Group_Energy_Invention', 'YEAR', 'JRC_Id',
    #                'NAME', 'Sector', 'Country', 'NUTS1', 'NUTS2', 'NUTS3', 'MI_member', 'World_player',
    #                'Total_Invention', 'Energy_Union_Priority', 'Actions', 'Technology', 'Tech_UC',
    #                'Invention', 'Invention_Granted', 'Invention_High_Value', 'Invention_Citation',
    #                'Patent', 'Patent_Granted', 'RnD_MEUR', 'Equation']

    soeur_rnd = pd.read_excel(file_path,
                              sheet_name='SOEUR_RnD',
                              names=names_2019b,
                              na_values='#N/A',
                              dtype={
                                  **{col: str for col in ['YEAR', 'JRC_Id', 'NAME', 'Country', 'Technology',
                                                          'Actions', 'Energy_Union_Priority']
                                     },
                                  **{col: float for col in ['RnD_MEUR']}
                              }
                              )

    soeur_rnd.rename(columns={
        **{'YEAR': 'year'},
        **{'Group_Id': 'soeur_group_id', 'Group_Name': 'soeur_group_name', 'BvD_ID': 'soeur_group_bvd_id',
           'Group_Country': 'group_country_2DID_soeur', 'Sector_UC': 'sector_UC', 'Group_UC': 'group_UC',
           'RnD_Group_UC': 'rnd_group_UC'},
        **{'JRC_Id': 'soeur_sub_id', 'NAME': 'soeur_sub_name', 'Country': 'sub_country_2DID_soeur',
           'Technology': 'technology', 'Actions': 'action', 'Energy_Union_Priority': 'priority',
           'NUTS1': 'sub_NUTS1', 'NUTS2': 'sub_NUTS2', 'NUTS3': 'sub_NUTS3', 'RnD_MEUR': 'rnd_clean'}
    }, inplace=True)

    return soeur_rnd[
        ['year'] +
        ['soeur_group_id', 'soeur_group_name', 'soeur_group_bvd_id', 'group_country_2DID_soeur', 'sector_UC',
         'group_UC', 'rnd_group_UC'] +
        ['soeur_sub_id', 'soeur_sub_name', 'sub_country_2DID_soeur', 'sub_NUTS1', 'sub_NUTS2', 'sub_NUTS3',
         'technology', 'action', 'priority', 'rnd_clean']
        ]


def jrc004_mnc_from_xls(root,
                        file_name):
    print('Read jrc_004 excel table')

    file_path = root.joinpath(file_name)

    jrc004_mnc_cols = [
        'scoreboard_MNC_id',
        'scoreboard_company_name',
        'scoreboard_country_2DID',
        'scoreboard_company_industry',
        'open_refine_Company_name',
        'soeur_group_id',
        'soeur_group_name',
        'soeur_group_country_2DID',
        'FP_bvd_name',
        'FP_bvd_id',
        'scoreboard_ICB_A',
        'scoreboard_ICB_B',
        'scoreboard_ICB_id',
        'scoreboard_ICB_3_5Dcode',
        'scoreboard_ICB_3_name',
        'scoreboard_ICB_D',
        'FP_bvd_NACE_4Dcode_FP',
        'year',
        'scoreboard_rnd_mEUR',
        'scoreboard_net_sales_mEUR',
        'scoreboard_employees'
    ]

    # Check if countries are in country-table

    jrc004_mnc = pd.read_excel(
        file_path,
        sheet_name='JRC004_MNC',
        names=jrc004_mnc_cols,
        na_values='#N/A',
        dtype={
            **{col: str for col in jrc004_mnc_cols[:17]},
            **{col: float for col in jrc004_mnc_cols[-3:]}
        }).rename(columns={
            'scoreboard_company_name': 'scoreboard_name',
            'FP_bvd_name': 'current_bvd_name',
            'FP_bvd_id': 'current_bvd_id',
            'FP_bvd_NACE_4Dcode_FP': 'current_bvd_NACE_4Dcode_FP'
        })

    mnc_table = jrc004_mnc[jrc004_mnc_cols[1:17]].drop_duplicates(subset=['soeur_group_name'])
    mnc_table.dropna(subset=['soeur_group_name'], inplace=True)

    print('Save mnc_table excel table')

    # mnc_table.to_excel(
    #     root.joinpath(r'mapping\JRC004_MNC.xlsx'),
    #     columns=jrc004_mnc_cols[1:17],
    #     float_format='%.10f',
    #     index=False,
    #     na_rep='#N/A'
    # )

    mnc_table.to_csv(
        root.joinpath(r'mapping\JRC004_MNC.csv'),
        columns=jrc004_mnc_cols[1:17],
        float_format='%.10f',
        index=False,
        na_rep='#N/A'
    )

    print('Save scoreboard_fins')

    scoreboard_fins = jrc004_mnc[[
        'scoreboard_MNC_id',
        'scoreboard_name',
        'year',
        'scoreboard_rnd_mEUR',
        'scoreboard_net_sales_mEUR',
        'scoreboard_employees'
    ]]

    scoreboard_fins.to_csv(
        root.joinpath(r'scoreboard_fins.csv'),
        columns=scoreboard_fins.columns,
        float_format='%.10f',
        index=False,
        na_rep='#N/A'
    )

    return mnc_table

from pathlib import Path


def mnc_soeur_rnd(root, file_name):

    print('Read SOEUR MNC RnD Exposure excel table')

    file_path = root.joinpath(file_name)

    mnc_table_cols = ['soeur_group_id',
                      'soeur_group_name',
                      'soeur_group_country_2DID',
                      'soeur_group_world_player',
                      'icb_3_name',
                      'year',
                      'soeur_group_rnd',
                      'soeur_group_rnd_from_group_uc',
                      'soeur_group_rnd_from_sector_uc',
                      'soeur_group_norm',
                      'soeur_group_exposure_from_group_uc',
                      'soeur_group_exposure_from_sector_uc']

    # step if countries are in country-table

    mnc_table = pd.read_excel(
        file_path,
        sheet_name='MNCs_R&D_Exposure_v2',
        names=mnc_table_cols,
        na_values='#N/A',
        dtype={
            **{col: str for col in mnc_table_cols[:5]},
            **{col: float for col in mnc_table_cols[6:]}
        }
    )

    # mnc_table.set_index(keys='soeur_group_name', inplace=True)

    return mnc_table[['soeur_group_id',
                      'soeur_group_name',
                      'soeur_group_country_2DID',
                      'icb_3_name',
                      'year',
                      'soeur_group_rnd',
                      'soeur_group_rnd_from_group_uc',
                      'soeur_group_rnd_from_sector_uc',
                      'soeur_group_exposure_from_group_uc',
                      'soeur_group_exposure_from_sector_uc']]


def mnc_newapp_rnd(root, file_name, method):

    print('Read NewApp parent RnD csv table')

    file_path = root.joinpath(file_name)

    mnc_table_cols = [
        'bvd9', 'year', 'orbis_parent_oprev', 'orbis_parent_rnd', 'newapp_parent_exposure', 'newapp_parent_rnd_clean',
        'method'
    ]

    mnc_table = pd.read_csv(
        file_path,
        names=mnc_table_cols,
        header=0,
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    mnc_table = mnc_table[mnc_table['method'] == method]

    return mnc_table[mnc_table_cols[:-1]]