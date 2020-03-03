from pathlib import Path
import configparser
import pandas as pd
import os


def import_my_config(case, base, data):
    """
    Read config.ini
    :param case: name of the config.ini section to consider
    :param base: path (as a string) of folder containing config.ini
    :param data: root path (as a string) for the working folder for corresponding case
    :return: dictionary of configuration parameters
    """
    print('Import my config ...')

    # Import config parameters
    config = configparser.ConfigParser(
        converters={'list': lambda x: [i.strip() for i in x.split(',')]}
    )

    base_path = Path(base)
    data_path = Path(data)

    config.read(base_path.joinpath(r'config.ini'))

    my_config = {
        'MAPPING': Path(config.get(case, 'MAPPING')),
        'SCREENING_KEYS': config.getlist(case, 'SCREENING_KEYS'),
        'REGIONS': config.getlist(case, 'REGIONS'),
        'CASE_ROOT': data_path.joinpath(config.get(case, 'CASE_ROOT')),
        'YEAR_LASTAV': config.getint(case, 'YEAR_LASTAV'),
        'SUBS_ID_FILE_N': config.getint(case, 'SUBS_ID_FILE_N'),
        'SUBS_FIN_FILE_N': config.getint(case, 'SUBS_FIN_FILE_N'),
        'MAIN_COMPS_FIN_FILE_N': config.getint(case, 'MAIN_COMPS_FIN_FILE_N'),
        'METHOD': config.get(case, 'METHOD'),
        'BASE': base_path
    }

    return my_config


def create_country_map(mapping, case_root):
    """
    Create a country mapping table from reference file
    :param mapping: path to the country mapping table file
    :param case_root: path of the working folder for the use case
    :return: Nothing
    """
    print('Read country mapping table ...')

    # Read Country mapping file
    country_map = pd.read_excel(mapping.joinpath(r'Mapping_Country.xlsx'),
                                sheet_name='Country_map',
                                names=['Country_Name_ISO', 'Country_Name_Simple', 'Country_2DID_ISO',
                                       'Country_3DID_ISO',
                                       'Is_OECD', 'Is_IEA', 'Is_MI', 'Region', 'IEA_Region', 'World_Player'
                                       ],
                                na_values='n.a.',
                                dtype={
                                    **{col: str for col in
                                       ['Country_Name_ISO', 'Country_Name_Simple', 'Country_2DID_ISO',
                                        'Country_3DID_ISO', 'Region', 'IEA_Region', 'World_Player'
                                        ]},
                                    **{col: bool for col in ['Is_OECD', 'Is_IEA', 'Is_MI']}
                                }
                                ).drop(columns='Region')

    print('Save country mapping table ...')
    print(case_root.joinpath(r'Mapping\Country_table.csv'))

    # Save it as csv
    os.mkdir(case_root.joinpath(r'Mapping'))

    country_map.to_csv(case_root.joinpath(r'Mapping\Country_table.csv'),
                       index=False,
                       columns=['Country_Name_ISO', 'Country_Name_Simple', 'Country_2DID_ISO', 'Country_3DID_ISO',
                                'Is_OECD', 'Is_IEA', 'Is_MI', 'IEA_Region', 'World_Player'
                                ],
                       float_format='%.10f',
                       na_rep='n.a.'
                       )


def select_main(case_root, year_lastav, regions, country_map):
    """
    Select the main companies by region (having invested the most in R&D since 2010 and corresponding together to more
    than 99% of R&D investments in the region) and consolidate a global list of unique companies
    :param case_root: path of the working folder for the use case
    :param year_lastav: most recent year to consider for R&D expenditures
    :param regions: list of regions considered to collect the input data
    :param country_map: dataframe for country mapping
    :return: analytical report
    """
    # Initialize DFs
    all_comps = pd.DataFrame()
    main_comps = pd.DataFrame()
    report = pd.DataFrame()
    i = 0

    print('Read input table ...')

    for region in regions:
        i+=1
        print(region + ' (File #' + str(i) + '/' + str(len(regions)) + ')')

        # Read input list of companies by world region
        df = pd.read_excel(case_root.joinpath(r'Input\Listed companies - ' + region + '.xlsx'),
                           sheet_name='Results',
                           names=['Rank', 'Company_name', 'BvD9', 'BvD_id', 'Country_2DID_ISO'] + ['RnD_Y' + str(YY) for
                                                                                                   YY in
                                                                                                   range(10, 19)[::-1]],
                           na_values='n.a.',
                           dtype={
                               **{col: str for col in ['Company_name', 'BvD9', 'BvD_id', 'Country_2DID_ISO']},
                               **{col: float for col in ['RnD_Y' + str(YY) for YY in range(10, 20)]}
                           }
                           ).drop(columns='Rank')

        df['Y_LastAv'] = year_lastav

        df['RnD_mean'] = df[['RnD_Y' + str(YY) for YY in range(10, 19)]].mean(axis=1, skipna=True)

        df['RnD_Y_LastAv'] = df['RnD_Y' + str(abs(year_lastav) % 100)]

        # Identify the top companies that constitute 99% of the R&D expenses
        start = 0.0
        count = 0

        while start < 0.99 * df['RnD_mean'].sum():
            count += 1
            start = df.nlargest(count, ['RnD_mean'])['RnD_mean'].sum()

        main_comps_region = df.nlargest(count, ['RnD_mean'])

        # main_comps_region['Region'] = region

        # Calculates main regional statistics
        region_report = pd.DataFrame({'Total_BvD9': df['BvD9'].count().sum(),
                                      '<Total_RnD_Y10_Y18>': df['RnD_mean'].sum(),
                                      'Selected_BvD9': main_comps_region['BvD9'].count().sum(),
                                      '<Selected_RnD_Y10_Y18>': main_comps_region['RnD_mean'].sum()
                                      }, index=[region])

        # Consolidate statistics and list of top R&D performers over different regions
        all_comps = all_comps.append(df)
        main_comps = main_comps.append(main_comps_region)

        report = report.append(region_report)
        report.index.name = region

    print('Clean output table ...')

    # Drop duplicates
    main_comps_clean = main_comps.drop_duplicates(subset='BvD9', keep='first')

    # Update report statistics
    region_report = pd.DataFrame({'Total_BvD9': all_comps['BvD9'].count().sum(),
                                  '<Total_RnD_Y10_Y18>': all_comps['RnD_mean'].sum(),
                                  'Selected_BvD9': main_comps_clean['BvD9'].count().sum(),
                                  '<Selected_RnD_Y10_Y18>': main_comps_clean['RnD_mean'].sum()
                                  }, index=['Total'])

    report = report.append(region_report)

    print('Merging with country_map ...')

    # Merging group country_map for allocation to world player categories
    merged = pd.merge(
        main_comps_clean, country_map[['Country_2DID_ISO', 'Country_3DID_ISO', 'World_Player']],
        left_on='Country_2DID_ISO', right_on='Country_2DID_ISO',
        how='left',
        suffixes=(False, False)
    )

    print('Saving main companies output file ...')

    # Save output table of selected main companies
    merged.to_csv(case_root.joinpath(r'Listed companies.csv'),
                  index=False,
                  columns=['BvD9', 'BvD_id', 'Company_name', 'Country_3DID_ISO', 'World_Player',
                           'RnD_mean', 'Y_LastAv', 'RnD_Y_LastAv'],
                  float_format='%.10f',
                  na_rep='n.a.'
                  )

    return report


def load_main_comps_fin(case_root, year_lastav, main_comps_fin_file_n, select_comps):
    """
    Loads financials for main companies
    :param case_root: path of the working folder for the use case
    :param year_lastav: most recent year to consider for R&D expenditures
    :param main_comps_fin_file_n: Number of input files to consolidate
    :return: Analytical report
    """
    main_comps_fin = pd.DataFrame()
    report = pd.DataFrame()

    print('Read main companies financials input tables')

    # Read ORBIS input list for groups financials
    for number in list(range(1, main_comps_fin_file_n + 1)):
        print('File #' + str(number) + '/' + str(main_comps_fin_file_n))
        df = pd.read_excel(
            case_root.joinpath(r'Input\Listed companies - financials #' + str(number) + '.xlsx'),
            sheet_name='Results',
            names=['Rank', 'Company_name', 'BvD9', 'BvD_id', 'Country_ISO', 'NACE_Code', 'NACE_desc', 'Year_LastAv']
                  + ['RnD_Y_LastAv', 'Emp_number', 'OpRev_Y_LastAv', 'NetSales_Y_LastAv']
                  + ['RnD_Y' + str(YY) for YY in range(10, 20)[::-1]],
            na_values='n.a.',
            dtype={
                **{col: str for col in ['Company_name', 'BvD9', 'BvD_id', 'Country_ISO', 'NACE_Code', 'NACE_desc']},
                **{col: float for col in ['RnD_Y_LastAv', 'OpRev_Y_LastAv', 'NetSales_Y_LastAv']
                   + ['RnD_Y' + str(YY) for YY in range(10, 20)]
                   }
            }
        ).drop(columns=['Rank', 'Country_ISO', 'NACE_Code', 'NACE_desc', 'Year_LastAv'])

        # Consolidate subsidiaries financials
        main_comps_fin = main_comps_fin.append(df)

    main_comps_fin = main_comps_fin.dropna(subset=['RnD_Y' + str(YY) for YY in range(10, 20)], how='all')

    report = report.append(
        pd.DataFrame(
            {'Selected_BvD9': main_comps_fin['BvD9'].nunique(),
             'Selected_RnD_Y' + str(year_lastav)[-2:]: main_comps_fin['RnD_Y' + str(year_lastav)[-2:]].sum()
             }, index=['Initial']
        ))

    main_comps_fin = main_comps_fin[main_comps_fin['BvD9'].isin(select_comps['BvD9'])]

    report = report.append(pd.DataFrame(
        {'Selected_BvD9': main_comps_fin['BvD9'].nunique(),
         'Selected_RnD_Y' + str(year_lastav)[-2:]: main_comps_fin['RnD_Y' + str(year_lastav)[-2:]].sum()
         }, index=['Selected']
    ))

    # Save it as csv
    main_comps_fin.to_csv(case_root.joinpath(r'Listed companies - financials.csv'),
                          index=False,
                          float_format='%.10f',
                          na_rep='n.a.'
                          )

    return report


def select_subs(case_root, subs_id_file_n):
    """
    Consolidate a unique list of subsidiaries
    :param case_root: path of the working folder for the use case
    :param subs_id_file_n: Number of input files to consolidate
    :return: analytical report
    """
    # Initialize DF
    subs = pd.DataFrame()

    print('Read subsidiary input tables')

    # Read ORBIS input list for subsidiaries
    for number in list(range(1, subs_id_file_n + 1)):
        print('File #' + str(number) + '/' + str(subs_id_file_n))
        df = pd.read_excel(case_root.joinpath(r'Input\Listed companies subsidiaries #' + str(number) + '.xlsx'),
                           sheet_name='Results',
                           na_values='No data fulfill your filter criteria',
                           names=['Rank', 'Company_name', 'BvD9', 'BvD_id', 'Group_Subs_Count', 'Subsidiary_name',
                                  'Sub_BvD9', 'Sub_BvD_id', 'Subs_lvl'],
                           dtype={
                               **{col: str for col in
                                  ['Rank', 'Company_name', 'BvD9', 'BvD_id', 'Subsidiary_name', 'Sub_BvD9',
                                   'Sub_BvD_id']},
                               'Group_Subs_Count': pd.Int64Dtype(),
                               'Subs_lvl': pd.Int8Dtype()}
                           ).drop(columns=['Rank', 'Subs_lvl', 'Group_Subs_Count'])

        # Consolidate list of subsidiaries
        subs = subs.append(df)

    # Drops not BVd identified subsidiaries and (group,subs) duplicates
    subs_clean = subs.dropna(subset=['BvD9', 'Sub_BvD9']).drop_duplicates(['BvD9', 'Sub_BvD9'], keep='first')

    report = pd.DataFrame({'Selected_BvD9': subs['BvD9'].nunique(),
                           'Selected_Sub_BvD9': subs_clean['Sub_BvD9'].nunique(),
                           'Duplicate_Sub_BvD9': subs_clean.duplicated(subset='Sub_BvD9', keep=False).sum()
                           }, index=['Initial set'])

    print('Save subsidiaries output file ...')

    # Save it as csv
    subs_clean.to_csv(case_root.joinpath(r'Listed companies subsidiaries.csv'),
                      index=False,
                      columns=['Company_name', 'BvD9', 'BvD_id', 'Subsidiary_name', 'Sub_BvD9', 'Sub_BvD_id'
                               ],
                      na_rep='n.a.'
                      )

    return report


def filter_comps_and_subs(case_root, select_subs, subs_fin):
    """
    Add bolean masks for the implementation of different RnD calculation method
    keep_all: Keep all main companies and all subsidiaries
    keep_comps: Keep all main companies and exclude subsidiaries that are main companies from subsidiaries list
    keep_subs: Exclude main companies that are a subsidiary from companies list and keep all subsidiaries
    :param case_root: path of the working folder for the use case
    :param select_subs: Consolidated dataframe of subsidiary identification and mapping to companies
    :return: analytical report
    """
    report = pd.DataFrame()

    print('Screen companies and subsidiaries lists')

    # Flag main companies that are a subsidiary of another main company and vice versa
    select_subs['is_comp_a_sub'] = select_subs['BvD9'].isin(select_subs['Sub_BvD9'])
    select_subs['is_sub_a_comp'] = select_subs['Sub_BvD9'].isin(select_subs['BvD9'])
    select_subs['has_fin'] = select_subs['Sub_BvD9'].isin(subs_fin['BvD9'])

    # Flag subsidiaries that are subsidiaries of multiple main companies
    select_subs['is_sub_a_duplicate'] = select_subs.duplicated(subset='Sub_BvD9', keep=False)

    print('Flag Keep all strategy')

    # Keep all main companies and all subsidiaries
    select_subs['keep_all'] = True

    report = report.append(
        pd.DataFrame({'Selected_BvD9': select_subs['BvD9'][select_subs['keep_all'] == True].nunique(),
                      'Selected_Sub_BvD9': select_subs['Sub_BvD9'][select_subs['keep_all'] == True].nunique(),
                      'Sub_BvD9_w_fin': select_subs['has_fin'][select_subs['keep_all'] == True].sum(),
                      'Dup_Sub_BvD9w_fin': select_subs['is_sub_a_duplicate'][
                          (select_subs['keep_all'] == True) & (select_subs['has_fin'] == True)].sum()
                      }, index=['keep_all']))

    print('Flag Keep comps strategy')

    # Keep all main companies and exclude subsidiaries that are main companies from subsidiaries list
    select_subs['keep_comps'] = select_subs['is_sub_a_comp'] == False

    report = report.append(
        pd.DataFrame({'Selected_BvD9': select_subs['BvD9'][select_subs['keep_comps'] == True].nunique(),
                      'Selected_Sub_BvD9': select_subs['Sub_BvD9'][
                          select_subs['keep_comps'] == True].nunique(),
                      'Sub_BvD9_w_fin': select_subs['has_fin'][select_subs['keep_comps'] == True].sum(),
                      'Dup_Sub_BvD9w_fin': select_subs['is_sub_a_duplicate'][
                          (select_subs['keep_comps'] == True) & (select_subs['has_fin'] == True)].sum()
                      }, index=['keep_comps']))

    print('Flag Keep subs strategy')

    # Exclude main companies that are a subsidiary from companies list and keep all subsidiaries
    select_subs['keep_subs'] = select_subs['is_comp_a_sub'] == False

    report = report.append(
        pd.DataFrame({'Selected_BvD9': select_subs['BvD9'][select_subs['keep_subs'] == True].nunique(),
                      'Selected_Sub_BvD9': select_subs['Sub_BvD9'][
                          select_subs['keep_subs'] == True].nunique(),
                      'Sub_BvD9_w_fin': select_subs['has_fin'][select_subs['keep_subs'] == True].sum(),
                      'Dup_Sub_BvD9w_fin': select_subs['is_sub_a_duplicate'][
                          (select_subs['keep_subs'] == True) & (select_subs['has_fin'] == True)].sum()
                      }, index=['Keep_subs']))

    print('Save companies and subsidiaries output files with filters ...')

    # Save it as csv
    select_subs.to_csv(case_root.joinpath(r'Listed companies subsidiaries - methods.csv'),
                       index=False,
                       na_rep='n.a.'
                       )

    return report


def load_subs_fin(case_root, subs_fin_file_n, select_subs):
    """
    Loads financials for subsidiaries
    :param case_root: path of the working folder for the use case
    :param year_lastav: most recent year to consider for R&D expenditures
    :param subs_fin_file_n: Number of input files to consolidate
    :return: Analytical report
    """
    subs_fin = pd.DataFrame()
    report = pd.DataFrame()

    print('Read subsidiaries financials input tables')

    # Read ORBIS input list for subsidiaries financials
    for number in list(range(1, subs_fin_file_n + 1)):
        print('File #' + str(number) + '/' + str(subs_fin_file_n))
        df = pd.read_excel(
            case_root.joinpath(r'Input\Listed companies subsidiaries - financials #' + str(number) + '.xlsx'),
            sheet_name='Results',
            names=['Rank', 'Company_name', 'BvD9', 'BvD_id', 'Country_ISO', 'NACE_Code', 'NACE_desc', 'Year_LastAvail']
                  + ['OpRevY' + str(YY) for YY in range(10, 20)[::-1]]
                  + ['Trade_desc', 'Prod&Serv_desc', 'FullOverview_desc'],
            na_values='n.a.',
            dtype={
                **{col: str for col in ['Company_name', 'BvD9', 'BvD_id', 'Country_ISO', 'NACE_Code', 'NACE_desc',
                                        'Trade_desc', 'Prod&Serv_desc', 'FullOverview_desc']},
                **{col: float for col in ['OpRevY' + str(YY) for YY in range(10, 20)[::-1]]}
            }
        ).drop(columns=['Rank', 'Country_ISO', 'NACE_Code', 'NACE_desc', 'Year_LastAvail'])

        # Consolidate subsidiaries financials
        subs_fin = subs_fin.append(df)

    report = report.append(
        pd.DataFrame(
            {'Selected_BvD9': 'n.a.',
             'Selected_Sub_ BvD9': subs_fin['BvD9'].count().sum(),
             'Duplicate_Sub_BvD9': subs_fin['BvD9'].duplicated().sum()
             }, index=['From input files']
        ))

    subs_fin = subs_fin.drop_duplicates('BvD9').dropna(subset=['OpRevY' + str(YY) for YY in range(10, 20)[::-1]],
                                                       how='all')

    report = report.append(
        pd.DataFrame(
            {'Selected_BvD9': 'n.a.',
             'Selected_Sub_ BvD9': subs_fin['BvD9'].count().sum(),
             'Duplicate_Sub_BvD9': subs_fin['BvD9'].duplicated().sum()
             }, index=['With financials']
        ))

    subs_fin = subs_fin[subs_fin['BvD9'].isin(select_subs['Sub_BvD9'])]

    report = report.append(
        pd.DataFrame(
            {'Selected_BvD9': 'n.a.',
             'Selected_Sub_ BvD9': subs_fin['BvD9'].count().sum(),
             'Duplicate_Sub_BvD9': subs_fin['BvD9'].duplicated().sum()
             }, index=['In select_subs']
        ))

    # Save it as csv
    subs_fin.to_csv(case_root.joinpath(r'Listed companies subsidiaries - financials.csv'),
                    index=False,
                    float_format='%.10f',
                    na_rep='n.a.'
                    )

    return report


def screen_subs(case_root, keywords, subs_fin, screening_keys):

    categories = list(keywords.keys())

    for category in categories:

        subs_fin[category] = False

        for keyword in keywords[category]:

            subs_fin[category] |= subs_fin['Trade_desc'].str.contains(keyword, case=False, regex=False) | \
                                  subs_fin['Prod&Serv_desc'].str.contains(keyword, case=False, regex=False) | \
                                  subs_fin['FullOverview_desc'].str.contains(keyword, case=False, regex=False)

    screen_subs = subs_fin.loc[:, ['Company_name', 'BvD9'] + categories]

    screen_subs['Sub_Turnover'] = subs_fin.loc[:, ['OpRevY' + str(YY) for YY in range(10, 20)]].sum(axis=1)

    screen_subs['Keyword_mask'] = list(
        map(bool, subs_fin[[cat for cat in categories if cat not in ['Generation', 'RnD']]].sum(axis=1)))

    report = pd.DataFrame({'#Subs': subs_fin['BvD9'].count().sum(),
                           '#Subs matching keywords': screen_subs.loc[
                               screen_subs['Keyword_mask'] == True, 'BvD9'].count().sum()
                           }, index=['Keyword'])

    # Save it as csv
    screen_subs.to_csv(case_root.joinpath(r'Listed companies subsidiaries - screening.csv'),
                       index=False,
                       columns=['BvD9'] + screening_keys,
                       float_format='%.10f',
                       na_rep='n.a.'
                       )

    return report