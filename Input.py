from pathlib import Path
import configparser
import pandas as pd
import os


def import_my_config(case, base):
    print('Import my config ...')

    # Import config parameters
    config = configparser.ConfigParser(
        converters={'list': lambda x: [i.strip() for i in x.split(',')]}
    )

    base_path = Path(base)

    config.read(base_path.joinpath(r'config.ini'))

    my_config = {
        'MAPPING': Path(config.get(case, 'MAPPING_PATH')),
        'SCREENING_KEYS': config.getlist(case, 'SCREENING_KEYS'),
        'REGION': config.getlist(case, 'ORBIS_REGION'),
        'CASE_ROOT': base_path.joinpath(config.get(case, 'CASE_ROOT_PATH')),
        'YEAR_LASTAV': config.getint(case, 'YEAR_LASTAV'),
        'SUBS_ID_FILE_N': config.getint(case, 'SUBS_ID_FILE_N'),
        'SUBS_FIN_FILE_N': config.getint(case, 'SUBS_FIN_FILE_N'),
        'GROUPS_FIN_FILE_N': config.getint(case, 'GROUPS_FIN_FILE_N'),
        'METHOD': config.get(case, 'SUBS_METHOD')
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
