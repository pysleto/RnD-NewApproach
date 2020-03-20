from pathlib import Path
import configparser
import pandas as pd
import os
import pprint as pp
import json
import numpy as np
from tabulate import tabulate


def convert(o):
    if isinstance(o, np.int32): return int(o)


def update(report, cases):
    """
    Update a json file with reporting outputs and pretty print a readable statistics report
    :param report: dictionary of reporting outputs
    :param cases: dictionary of configuration parameters for the considered use case
    :return: Nothing
    """

    #    print('Update report.json file ...')

    with open(cases['CASE_ROOT'].joinpath(r'report.json'), 'w') as file:
        json.dump(report, file, indent=4, default=convert)


def pprint(report, cases):
    """
    Pretty print a readable statistics report
    :param company_type:
    :param report: dictionary of reporting outputs
    :param cases: dictionary of configuration parameters for the considered use case
    :return: Nothing
    """

    with open(cases['CASE_ROOT'].joinpath(r'report.txt'), 'w') as file:
        file.write('INITIALISE\n\n')

        json.dump(report['initialisation'], file, indent=4, default=convert)

        file.write('\n\n')

        file.write('NB: RnD in EUR million\n\n')

        for company_type in cases['COMPANY_TYPES']:
            file.write('*********************************************\n')
            file.write(str(company_type.upper()) + '\n')
            file.write('*********************************************\n\n')

            file.write('SELECT MAIN COMPANIES\n\n')

            df = pd.DataFrame.from_dict(
                report['select_main_companies'], orient='index'
            ).append(
                pd.DataFrame.from_dict(
                    report['load_main_company_financials'], orient='index'
                )
            )

            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
            file.write('\n\n')

            file.write('LOAD SUBSIDIARIES FROM SELECTED MAIN COMPANIES\n\n')

            df = pd.DataFrame.from_dict(
                report['load_subsidiary_identification'], orient='index'
            ).append(
                pd.merge(
                    pd.DataFrame.from_dict(report['select_main_companies_and_subsidiaries'], orient='index'),
                    pd.DataFrame.from_dict(report['keyword_screen_by_method'], orient='index'),
                    left_index=True, right_index=True
                )
            )

            # .append(
            #     pd.merge(
            #         pd.DataFrame.from_dict(report['load_subsidiary_financials'], orient='index'),
            #         pd.DataFrame.from_dict(report['screen_subsidiary_activities'], orient='index'),
            #         left_index=True, right_index=True
            #         )
            #     )

            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
            file.write('\n\n')

            file.write('COMPUTE EXPOSURE\n\n')

            file.write('at_main_company_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_exposure']['at_main_company_level'], orient='index')
            file.write(
                tabulate(df, tablefmt='simple', headers=df.columns, floatfmt=('0.0f', '5.5f', '10,.0f', '10,.0f')))
            file.write('\n\n')

            file.write('at_subsidiary_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_exposure']['at_subsidiary_level'], orient='index')
            file.write(
                tabulate(df, tablefmt='simple', headers=df.columns, floatfmt=('0.0f', '5.5f', '10,.0f', '10,.0f')))
            file.write('\n\n')

            file.write('COMPUTE RND\n\n')

            file.write('at_main_company_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_rnd']['at_main_company_level'])
            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
            file.write('\n\n')

            file.write('at_subsidiary_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_rnd']['at_subsidiary_level'])
            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))


def consolidate_new_approach_rnd_estimates(cases, files):
    """

    :param sub_rnd:
    :param cases:
    :param company_type:
    :return:
    """

    output = pd.DataFrame()

    sub_rnd = sub_rnd_conso = main_comp_rnd = main_comp_rnd_conso = pd.DataFrame()

    print('consolidate_newapproach_rnd_estimates')

    # Consolidate output for new_approach
    for company_type in cases['COMPANY_TYPES']:
        print(company_type)
        print('... subsidiaries')

        sub_rnd = pd.read_csv(
            files['OUTPUT'][company_type]['RND_EXT']['SUBS'],
            na_values='n.a.',
            dtype={col: str for col in ['bvd9', 'sub_bvd9']}
        ).drop(columns=['bvd9', 'company_name', 'sub_bvd9', 'sub_company_name',
                        'sub_country_2DID_iso', 'sub_country_3DID_iso',
                        'sub_turnover', 'sub_turnover_masked', 'sub_exposure'
                        ]
               ).rename(columns={'sub_world_player': 'world_player', 'sub_rnd_final': 'rnd_final'}
                        )

        sub_rnd['company_type'] = company_type
        sub_rnd['approach'] = 'new_approach'

        sub_rnd_conso = sub_rnd_conso.append(sub_rnd)

        print('... main companies')

        main_comp_rnd = pd.read_csv(
            files['OUTPUT'][company_type]['RND_EXT']['MAIN_COMPS'],
            na_values='n.a.',
            dtype={col: str for col in ['bvd9', 'main_comp_bvd9']}
        ).drop(columns=['bvd9', 'company_name', 'main_comp_exposure', 'main_comp_rnd']
               ).rename(columns={'main_comp_rnd_final': 'rnd_final'}
                        )

        main_comp_rnd['company_type'] = company_type
        main_comp_rnd['approach'] = 'new_approach'

        main_comp_rnd_conso = main_comp_rnd_conso.append(main_comp_rnd)

    # Consolidation across company types
    main_comp_rnd_conso = main_comp_rnd_conso.groupby(['year', 'world_player', 'approach', 'company_type', 'method'])[
        'rnd_final'].sum().reset_index()

    main_comp_rnd_conso['level'] = 'main_company'

    output = output.append(main_comp_rnd_conso)

    sub_rnd_conso = sub_rnd_conso.groupby(['year', 'world_player', 'approach', 'company_type', 'method'])[
        'rnd_final'].sum().reset_index()

    sub_rnd_conso['level'] = 'subsidiary'

    output = output.append(sub_rnd_conso)

    # Save output tables
    output.to_csv(files['FINAL']['CONSOLIDATED_RND_ESTIMATES'],
                  index=False,
                  float_format='%.10f',
                  na_rep='n.a.'
                  )

    return output


def benchmark(cases, files):
    """

    :param sub_rnd:
    :param cases:
    :param company_type:
    :return:
    """
    output = pd.DataFrame()

    soeur_rnd = pd.DataFrame()

    # Consolidate output for new_approach
    output = output.append(
        consolidate_new_approach_rnd_estimates(cases, files)
    )

    # Consolidate output for soeur_rnd approach

    print('benchmarking soeur_rnd')

    soeur_rnd = pd.read_csv(
        files['MAPPING']['SOEUR_RND_REFERENCE_PATH'],
        na_values='n.a.',
        dtype={
            col: str for col in ['year']
        }
    )

    soeur_rnd = soeur_rnd.groupby(['year', 'world_player'])[
        'rnd_final'].sum().reset_index()

    soeur_rnd['approach'] = files['MAPPING']['SOEUR_RND_VERSION']
    soeur_rnd['company_type'] = 'n.a.'
    soeur_rnd['level'] = 'subsidiary'
    soeur_rnd['method'] = 'keep_subs'

    output = output.append(soeur_rnd)

    # Save output tables
    output.to_csv(files['FINAL']['BENCH'],
                  index=False,
                  float_format='%.10f',
                  na_rep='n.a.'
                  )
