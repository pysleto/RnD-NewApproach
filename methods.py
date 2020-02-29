from pathlib import Path
import configparser
import pandas as pd
import os


def import_my_config(case, base, data):
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
        'GROUPS_FIN_FILE_N': config.getint(case, 'GROUPS_FIN_FILE_N'),
        'METHOD': config.get(case, 'METHOD')
    }

    return my_config


def create_country_map(mapping, case_root):
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
    # Initialize DFs
    all_comps = pd.DataFrame()
    main_comps = pd.DataFrame()
    report = pd.DataFrame()

    print('Read input table ...')

    for region in regions:

        print(region)

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
                                      'Total_RnD': df['RnD_mean'].sum(),
                                      'Selected_BvD9': main_comps_region['BvD9'].count().sum(),
                                      'Selected_RnD': main_comps_region['RnD_mean'].sum()
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
                                  'Total_RnD': all_comps['RnD_mean'].sum(),
                                  'Selected_BvD9': main_comps_clean['BvD9'].count().sum(),
                                  'Selected_RnD': main_comps_clean['RnD_mean'].sum()
                                  }, index=['Total'])

    report = report.append(region_report)
#    report.index.name = 'Total'

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


def select_subs(case_root, subs_id_file_n):
    # Initialize DF
    subs = pd.DataFrame()

    print('Read subsidiary input tables')

    # Read ORBIS input list for subsidiaries
    for number in list(range(1, subs_id_file_n + 1)):
        print('File #' + str(number))
        df = pd.read_excel(case_root.joinpath(r'Input\Listed companies subsidiaries #' + str(number) + '.xlsx'),
                           sheet_name='Results',
                           na_values='No data fulfill your filter criteria',
                           names=['Rank', 'Company_name', 'BvD9', 'BvD_id', 'Group_Subs_Count', 'Sub_BvD_id',
                                  'Sub_BvD9', 'Subs_lvl'],
                           dtype={
                               **{col: str for col in
                                  ['Rank', 'Company_name', 'BvD9', 'BvD_id', 'Sub_BvD9', 'Sub_BvD_id']},
                               'Group_Subs_Count': pd.Int64Dtype(),
                               'Subs_lvl': pd.Int8Dtype()}
                           ).drop(columns=['Rank', 'Subs_lvl', 'Group_Subs_Count'])

        # Consolidate list of subsidiaries
        subs = subs.append(df)

    # Drops not BVd identified subsidiaries and (group,subs) duplicates
    subs_clean = subs.dropna(subset=['BvD9', 'Sub_BvD9']).drop_duplicates(['BvD9', 'Sub_BvD9'], keep='first')

    report = pd.DataFrame({'Total_BvD9': subs['BvD9'].nunique(),
                           'Total_Sub_BvD9': subs['Sub_BvD9'].nunique(),
                           'Selected_Sub_BvD9': subs_clean['Sub_BvD9'].nunique()
                           }, index=['Subsidiaries from selected main companies'])

    print('Saving subsidiaries output file ...')

    # Save it as csv
    subs_clean.to_csv(case_root.joinpath(r'Listed companies subsidiaries.csv'),
                      index=False,
                      columns=['Company_name', 'BvD9', 'BvD_id', 'Sub_BvD9', 'Sub_BvD_id'
                               ],
                      na_rep='n.a.'
                      )

    return report