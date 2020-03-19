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


def header(report, cases):
    """
    Pretty print a report header
    :param report: dictionary of reporting outputs
    :param cases: dictionary of configuration parameters for the considered use case
    :return: Nothing
    """

    with open(cases['CASE_ROOT'].joinpath(r'report.txt'), 'w') as file:
        file.write('INITIALISE\n\n')

        json.dump(report['initialisation'], file, indent=4, default=convert)

        file.write('\n\n')

        file.write('NB: RnD in EUR million\n\n')


def pprint(report, cases, company_type):
    """
    Pretty print a readable statistics report
    :param report: dictionary of reporting outputs
    :param cases: dictionary of configuration parameters for the considered use case
    :return: Nothing
    """

    with open(cases['CASE_ROOT'].joinpath(r'report.txt'), 'w') as file:
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
        file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt=('0.0f', '5.5f', '10,.0f', '10,.0f')))
        file.write('\n\n')

        file.write('at_subsidiary_level\n\n')

        df = pd.DataFrame.from_dict(report['compute_exposure']['at_subsidiary_level'], orient='index')
        file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt=('0.0f', '5.5f', '10,.0f', '10,.0f')))
        file.write('\n\n')

        file.write('COMPUTE RND\n\n')

        file.write('at_main_company_level\n\n')

        df = pd.DataFrame.from_dict(report['compute_rnd']['at_main_company_level'])
        file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
        file.write('\n\n')

        file.write('at_subsidiary_level\n\n')

        df = pd.DataFrame.from_dict(report['compute_rnd']['at_subsidiary_level'])
        file.write(tabulate(df, tablefmt='simple', headers=df.columns, floatfmt='10,.0f'))
