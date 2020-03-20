# Import libraries
import pandas as pd
import method as mtd
import config as cfg
import report as rpt
import datetime
import json
from pathlib import Path

# <editor-fold desc="STEP #0 - Initialisation">

print('STEP #0 - Initialisation')

# Set initial parameters
print('Read Configuration parameters ...')

# Load config files
(cases, cases_as_strings, files, use_case, place) = cfg.init()

# Initialize report
report = {}

if cases['CASE_ROOT'].joinpath(r'report.json').exists():
    # Load existing file
    with open(cases['CASE_ROOT'].joinpath(r'report.json'), 'r') as file:
        report = json.load(file)

    # Update time stamp
    report['initialisation']['Datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
else:
    report['initialisation'] = {
        'Datetime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'Place': place,
        'Use case': cases_as_strings
    }

rpt.update(report, cases)

# Load keywords for activity screening
with open(cases['BASE'].joinpath(r'keywords.json'), 'r') as file:
    keywords = json.load(file)

# Import mapping tables
print('Read country_table.csv - Country mapping table ...')
country_map = pd.read_csv(files['MAPPING']['COUNTRY_REFERENCE_PATH'])

# Initialize final consolidation
sub_rnd = pd.DataFrame()
# </editor-fold>

for company_type in cases['COMPANY_TYPES']:

    # <editor-fold desc="STEP #1 - Selection of main companies">

    print('STEP #1 - Selection of main companies and consolidation of their financials')

    # Select main companies by world region
    if not files['OUTPUT'][company_type]['ID_EXT']['MAIN_COMPS'].exists():
        report['select_main_companies'] = mtd.select_main(cases, files, country_map, company_type)
        rpt.update(report, cases)

    print('Read selected listed companies ...')
    select_comp = pd.read_csv(
        files['OUTPUT'][company_type]['ID_EXT']['MAIN_COMPS'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'bvd_id']
        }
    )

    # Load main companies financials
    if not files['OUTPUT'][company_type]['FIN_EXT']['MAIN_COMPS'].exists():
        report['load_main_company_financials'] = mtd.load_main_comp_fin(cases, files, select_comp, country_map,
                                                                        company_type)
        rpt.update(report, cases)

    print('Read selected main companies financials ...')
    main_comp_fin = pd.read_csv(
        files['OUTPUT'][company_type]['FIN_EXT']['MAIN_COMPS'],
        na_values='n.a.',
        # usecols=['bvd9', 'company_name'] + ['rnd_y' + str(YY) for YY in range(10, 20)[::-1]],
        dtype={
            col: str for col in ['bvd9']
        }
    )
    # </editor-fold>

    # <editor-fold desc="STEP #2 - Consolidation of subsidiaries and main companies">

    print('STEP #2 - Consolidation of subsidiaries and main companies')

    # Load list of subsidiaries
    if not files['OUTPUT'][company_type]['ID_EXT']['SUBS'].exists():
        report['load_subsidiary_identification'] = mtd.select_subs(cases, files, company_type)
        rpt.update(report, cases)

    print('Read listed companies subsidiaries.csv ...')
    select_subs = pd.read_csv(
        files['OUTPUT'][company_type]['ID_EXT']['SUBS'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id']
        }
    )

    # Load subsidiaries financials
    if not files['OUTPUT'][company_type]['FIN_EXT']['SUBS'].exists():
        report['load_subsidiary_financials'] = mtd.load_subs_fin(cases, files, select_subs, country_map, company_type)
        rpt.update(report, cases)

    print('Read listed companies subsidiaries - financials.csv ...')
    subs_fin = pd.read_csv(
        files['OUTPUT'][company_type]['FIN_EXT']['SUBS'],
        na_values='n.a.',
        dtype={
            col: str for col in ['sub_bvd9', 'sub_bvd_id']
        }
    )

    # Analyze main companies and subsidiaries
    if not files['OUTPUT'][company_type]['METHOD_EXT']['SUBS'].exists():
        report['select_main_companies_and_subsidiaries'] = mtd.filter_comps_and_subs(cases, files, select_subs,
                                                                                     subs_fin, company_type)
        rpt.update(report, cases)

    print('Read listed companies subsidiaries - methods.csv ...')

    select_subs = pd.read_csv(
        files['OUTPUT'][company_type]['METHOD_EXT']['SUBS'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        },
        usecols=['company_name', 'bvd9', 'sub_company_name', 'sub_bvd9'] + cases['METHODS']
    )
    # </editor-fold>

    # <editor-fold desc="STEP #3 - Keyword screening of subsidiaries activities">

    print('STEP #3 - Keyword screening of subsidiaries activities')

    # Screen keywords in subsidiaries activity description fields and calculate subsidiary exposure
    if not files['OUTPUT'][company_type]['SCREEN_EXT']['SUBS'].exists():
        report['screen_subsidiary_activities'] = mtd.screen_subs(cases, files, keywords, subs_fin, company_type)
        rpt.update(report, cases)

    print('Read listed companies subsidiaries - Screening.csv ...')

    screen_subs = pd.read_csv(
        files['OUTPUT'][company_type]['SCREEN_EXT']['SUBS'],
        na_values='n.a.',
        dtype={
            col: str for col in ['sub_bvd9', 'sub_bvd_id']
        },
        usecols=['sub_bvd9', 'keyword_mask', 'sub_turnover_masked', 'sub_turnover']
    )
    # </editor-fold>

    # <editor-fold desc="STEP #4 - Calculating group and subsidiary level exposure">

    print('STEP #4 - Calculating group and subsidiary level exposure')

    # Loading exposure at subsidiary and main company level
    if not files['OUTPUT'][company_type]['EXPO_EXT']['SUBS'].exists():
        (report['keyword_screen_by_method'], report['compute_exposure']) = mtd.compute_sub_exposure(cases,
                                                                                                    files,
                                                                                                    select_subs,
                                                                                                    screen_subs,
                                                                                                    subs_fin,
                                                                                                    company_type)
        rpt.update(report, cases)

    print('Read ' + str(company_type) + ' - exposure.csv ...')

    main_comp_exposure = pd.read_csv(
        files['OUTPUT'][company_type]['EXPO_EXT']['MAIN_COMPS'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    print('Read ' + str(company_type) + ' subsidiaries - exposure.csv ...')

    sub_exposure = pd.read_csv(
        files['OUTPUT'][company_type]['EXPO_EXT']['SUBS'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )

    #
    if not files['OUTPUT'][company_type]['RND_EXT']['MAIN_COMPS'].exists():
        report['compute_rnd'] = {}
        report['compute_rnd']['at_main_company_level'] = mtd.compute_main_comp_rnd(cases, files, main_comp_exposure,
                                                                                   main_comp_fin, company_type)
        rpt.update(report, cases)

    print('Read ' + str(company_type) + ' - rnd estimates.csv ...')

    main_comp_rnd = pd.read_csv(
        files['OUTPUT'][company_type]['RND_EXT']['MAIN_COMPS'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    if not files['OUTPUT'][company_type]['RND_EXT']['SUBS'].exists():
        report['compute_rnd']['at_subsidiary_level'] = mtd.compute_sub_rnd(cases, files, sub_exposure, main_comp_rnd,
                                                                           company_type)
        rpt.update(report, cases)

    print('Read ' + str(company_type) + ' subsidiaries - rnd estimates.csv ...')

    sub_rnd = pd.read_csv(
        files['OUTPUT'][company_type]['RND_EXT']['SUBS'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )
    # </editor-fold>

# <editor-fold desc="STEP #4 - Final reporting and consolidation">

print('STEP #4 - Final reporting and consolidation')

with open(cases['CASE_ROOT'].joinpath(r'report.json'), 'r') as file:
    report = json.load(file)

rpt.benchmark(cases, files)

rpt.pprint(report, cases)
# </editor-fold>
