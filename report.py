import pandas as pd
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

            file.write('SELECT PARENT COMPANIES\n\n')

            df = pd.DataFrame.from_dict(
                report['select_parents'], orient='index'
            ).append(
                pd.DataFrame.from_dict(
                    report['load_parent_financials'], orient='index'
                )
            )

            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
            file.write('\n\n')

            file.write('LOAD SUBSIDIARIES FROM SELECTED PARENT COMPANIES\n\n')

            df = pd.DataFrame.from_dict(
                report['load_subsidiary_identification'], orient='index'
            ).append(
                pd.merge(
                    pd.DataFrame.from_dict(report['select_parents_and_subsidiaries'], orient='index'),
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

            file.write('at_parent_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_exposure']['at_parent_level'], orient='index')
            file.write(
                tabulate(df, tablefmt='simple', headers=df.columns, floatfmt=('0.0f', '5.5f', '10,.0f', '10,.0f')))
            file.write('\n\n')

            file.write('at_subsidiary_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_exposure']['at_subsidiary_level'], orient='index')
            file.write(
                tabulate(df, tablefmt='simple', headers=df.columns, floatfmt=('0.0f', '5.5f', '10,.0f', '10,.0f')))
            file.write('\n\n')

            file.write('COMPUTE RND\n\n')

            file.write('at_parent_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_rnd']['at_parent_level'])
            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
            file.write('\n\n')

            file.write('at_subsidiary_level\n\n')

            df = pd.DataFrame.from_dict(report['compute_rnd']['at_subsidiary_level'])
            file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))


def consolidate_new_approach_rnd_estimates(cases, files):
    """
    :param cases:
    :param files:
    :return:
    """

    output = pd.DataFrame()

    sub_rnd = sub_rnd_conso = parent_rnd = parent_rnd_conso = pd.DataFrame()

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

        print('... PARENT COMPANIES')

        parent_rnd = pd.read_csv(
            files['OUTPUT'][company_type]['RND_EXT']['PARENTS'],
            na_values='n.a.',
            dtype={col: str for col in ['bvd9', 'parent_bvd9']}
        ).drop(columns=['bvd9', 'company_name', 'parent_exposure', 'parent_rnd']
               ).rename(columns={'parent_rnd_final': 'rnd_final'}
                        )

        parent_rnd['company_type'] = company_type
        parent_rnd['approach'] = 'new_approach'

        parent_rnd_conso = parent_rnd_conso.append(parent_rnd)

    # Consolidation across company types
    parent_rnd_conso = parent_rnd_conso.groupby(['year', 'world_player', 'approach', 'company_type', 'method'])[
        'rnd_final'].sum().reset_index()

    parent_rnd_conso['level'] = 'parent'

    output = output.append(parent_rnd_conso)

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
    :param cases:
    :param files:
    :return:
    """
    output = pd.DataFrame()

    soeur_rnd = soeur_rnd_conso = pd.DataFrame()

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

    for method in cases['METHODS']:
        soeur_rnd['method'] = method
        soeur_rnd_conso = soeur_rnd_conso.append(soeur_rnd)

    output = output.append(soeur_rnd_conso)

    # Consolidate output for new_approach
    output = output.append(
        consolidate_new_approach_rnd_estimates(cases, files)
    )

    # Save output tables
    output.to_csv(files['FINAL']['BENCH'],
                  index=False,
                  float_format='%.10f',
                  na_rep='n.a.'
                  )
