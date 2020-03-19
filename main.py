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
use_case = 'EU_28'
place = 'home'
base_path = Path(r'C:\Users\letousi\PycharmProjects\rnd-NewApproach')
data_path = Path(r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020')
map_source_path = Path(r'\\net1.cec.eu.int\jrc-services\PTT-Users\letousi\My Documents\mapping_tables')

if place == 'home':
    base_path = Path(r'C:\Users\Simon\Documents\PycharmProjects\rnd-NewApproach')
    data_path = base_path

print('Read Configuration parameters ...')

# Load config files
cases, cases_as_strings = cfg.import_my_cases(use_case, base_path, data_path)

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

rpt.header(report, cases)

# Load keywords for activity screening
with open(cases['BASE'].joinpath(r'keywords.json'), 'r') as file:
    keywords = json.load(file)

# Import mapping tales
map_output_path = cases['CASE_ROOT'].joinpath(r'mapping\country_tables.csv')

if not map_output_path.exists():
    mtd.create_country_map(cases, map_source_path, map_output_path)

print('Read country_table.csv - Country mapping table ...')
country_map = pd.read_csv(map_output_path)
# </editor-fold>

for company_type in cases['COMPANY_TYPES']:

    files = cfg.import_my_files(cases, company_type, base_path, data_path)

    # <editor-fold desc="STEP #1 - Selection of main companies">

    print('STEP #1 - Selection of main companies and consolidation of their financials')

    # Select main companies by world region
    if not files['MAIN_COMPS']['ID_EXT'].exists():
        report['select_main_companies'] = mtd.select_main(cases, files, country_map)
        rpt.update(report, cases)

    print('Read selected listed companies ...')
    select_comp = pd.read_csv(
        files['MAIN_COMPS']['ID_EXT'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'bvd_id']
        }
    )

    # Load main companies financials
    if not files['MAIN_COMPS']['FIN_EXT'].exists():
        report['load_main_company_financials'] = mtd.load_main_comp_fin(cases, files, select_comp)
        rpt.update(report, cases)

    print('Read selected main companies financials ...')
    main_comp_fin = pd.read_csv(
        files['MAIN_COMPS']['FIN_EXT'],
        na_values='n.a.',
        usecols=['bvd9', 'company_name'] + ['rnd_y' + str(YY) for YY in range(10, 20)[::-1]],
        dtype={
            col: str for col in ['bvd9']
        }
    )
    # </editor-fold>

    # <editor-fold desc="STEP #2 - Consolidation of subsidiaries and main companies">

    print('STEP #2 - Consolidation of subsidiaries and main companies')

    # Load list of subsidiaries
    if not files['SUBS']['ID_EXT'].exists():
        report['load_subsidiary_identification'] = mtd.select_subs(cases, files)
        rpt.update(report, cases)

    print('Read listed companies subsidiaries.csv ...')
    select_subs = pd.read_csv(
        files['SUBS']['ID_EXT'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id']
        }
    )

    # Load subsidiaries financials
    if not files['SUBS']['FIN_EXT'].exists():
        report['load_subsidiary_financials'] = mtd.load_subs_fin(cases, files, select_subs, country_map)
        rpt.update(report, cases)

    print('Read listed companies subsidiaries - financials.csv ...')
    subs_fin = pd.read_csv(
        files['SUBS']['FIN_EXT'],
        na_values='n.a.',
        dtype={
            col: str for col in ['sub_bvd9', 'sub_bvd_id']
        }
    )

    # Analyze main companies and subsidiaries
    if not files['SUBS']['METHOD_EXT'].exists():
        report['select_main_companies_and_subsidiaries'] = mtd.filter_comps_and_subs(cases, files, select_subs,
                                                                                     subs_fin)
        rpt.update(report, cases)

    print('Read listed companies subsidiaries - methods.csv ...')

    select_subs = pd.read_csv(
        files['SUBS']['METHOD_EXT'],
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
    if not files['SUBS']['SCREEN_EXT'].exists():
        report['screen_subsidiary_activities'] = mtd.screen_subs(cases, files, keywords, subs_fin)
        rpt.update(report, cases)

    print('Read listed companies subsidiaries - Screening.csv ...')

    screen_subs = pd.read_csv(
        files['SUBS']['SCREEN_EXT'],
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
    if not files['SUBS']['EXPO_EXT'].exists():
        (report['keyword_screen_by_method'], report['compute_exposure']) = mtd.compute_sub_exposure(cases,
                                                                                                    files,
                                                                                                    select_subs,
                                                                                                    screen_subs,
                                                                                                    subs_fin)
        rpt.update(report, cases)

    print('# Read ' + str(company_type) + ' - exposure.csv ...')

    main_comp_exposure = pd.read_csv(
        files['MAIN_COMPS']['EXPO_EXT'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    print('# Read ' + str(company_type) + ' subsidiaries - exposure.csv ...')

    sub_exposure = pd.read_csv(
        files['SUBS']['EXPO_EXT'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )

    #
    if not files['MAIN_COMPS']['RND_EXT'].exists():
        report['compute_rnd'] = {}
        report['compute_rnd']['at_main_company_level'] = mtd.compute_main_comp_rnd(cases, files, main_comp_exposure,
                                                                                   main_comp_fin)
        rpt.update(report, cases)

    print('# Read ' + str(company_type) + ' - rnd estimates.csv ...')

    main_comp_rnd = pd.read_csv(
        files['MAIN_COMPS']['RND_EXT'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    if not files['SUBS']['RND_EXT'].exists():
        report['compute_rnd']['at_subsidiary_level'] = mtd.compute_sub_rnd(cases, files, sub_exposure, main_comp_rnd)
        rpt.update(report, cases)

    print('# Read ' + str(company_type) + ' subsidiaries - rnd estimates.csv ...')

    sub_rnd = pd.read_csv(
        files['SUBS']['RND_EXT'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9']
        }
    )
    # </editor-fold>

    with open(cases['CASE_ROOT'].joinpath(r'report.json'), 'r') as file:
        report = json.load(file)

    rpt.pprint(report, cases, company_type)
