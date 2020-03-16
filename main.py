# Import libraries
import pandas as pd
import methods as mtd
from tabulate import tabulate
import datetime
import json

# <editor-fold desc="STEP #0 - Initialisation">

print('STEP #0 - Initialisation')

# Set initial parameters
use_case = 'EU_28'
place = 'home'
company_type = 'listed companies'
base = r'C:\Users\letousi\PycharmProjects\rnd-NewApproach'
data = r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020'

if place == 'home':
    base = r'C:\Users\Simon\Documents\PyProjs\rnd-NewApproach'
    data = base

print('Read Configuration parameters ...')

# Load config files
cases = mtd.import_my_cases(use_case, base, data)
files = mtd.import_my_files(cases['CASE_ROOT'], base, data, company_type)

# Load keywords for activity screening
with open(cases['BASE'].joinpath(r'keywords.json'), 'r') as file:
    keywords = json.load(file)

if files['REPORT']['PATH'].exists():
    r = open(files['REPORT']['PATH'], 'a')
else:
    r = open(files['REPORT']['PATH'], 'w')
    r.write('Step #0 - Initialisation\n\n')
    r.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '\n\n')
    r.write('Place = ' + place + '\n' + 'Type = ' + company_type + '\n\n')

    for key, value in cases.items():
        r.write(key + ' = ' + str(value) + '\n')
    #    pprint(cases, stream=r)
    r.write('\n')

# Import mapping tales
if not files['MAP']['PATH'].exists():
    mtd.create_country_map(cases, files)

print('Read country_table.csv - Country mapping table ...')
country_map = pd.read_csv(files['MAP']['PATH'].joinpath(r'country_tables.csv'))
# </editor-fold>

# <editor-fold desc="STEP #1 - Selection of main companies">

print('STEP #1 - Selection of main companies and consolidation of their financials')

# Select main companies by world region
if not files['MAIN_COMPS']['PATH'].exists():
    report = mtd.select_main(cases, files, country_map)

    r.write('Step #1 - Selection of main companies\n\n')
    r.write(str(cases['YEAR_LASTAV']) + ' ¦ ' + company_type + ' ¦ ' + 'RnD in EUR million\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read selected listed companies ...')
select_comp = pd.read_csv(
    files['MAIN_COMPS']['PATH'],
    na_values='n.a.',
    dtype={
        col: str for col in ['bvd9', 'bvd_id']
    }
)

# Load main companies financials
if not files['MAIN_COMPS']['FIN_PATH'].exists():
    report = mtd.load_main_comp_fin(cases, files, select_comp)

    r.write('Companies with financials and declared RnD\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read selected main companies financials ...')
main_comp_fin = pd.read_csv(
    files['MAIN_COMPS']['FIN_PATH'],
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
if not files['SUBS']['PATH'].exists():
    report = mtd.select_subs(cases, files)

    r.write('Step #2 - Consolidation of subsidiaries from selected main companies\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read listed companies subsidiaries.csv ...')
select_subs = pd.read_csv(
    files['SUBS']['PATH'],
    na_values='n.a.',
    dtype={
        col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id']
    }
)

# Load subsidiaries financials
if not files['SUBS']['FIN_PATH'].exists():
    report = mtd.load_subs_fin(cases, files, select_subs, country_map)

    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read listed companies subsidiaries - financials.csv ...')
subs_fin = pd.read_csv(
    files['SUBS']['FIN_PATH'],
    na_values='n.a.',
    dtype={
        col: str for col in ['sub_bvd9', 'sub_bvd_id']
    }
)

# Analyze main companies and subsidiaries
if not files['SUBS']['METHOD_PATH'].exists():
    report = mtd.filter_comps_and_subs(cases, files, select_subs, subs_fin)

    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read listed companies subsidiaries - methods.csv ...')

select_subs = pd.read_csv(
    files['SUBS']['METHOD_PATH'],
    na_values='n.a.',
    dtype={
        col: str for col in ['sub_bvd9', 'sub_bvd_id']
    },
    usecols=['company_name', 'bvd9', 'sub_company_name', 'sub_bvd9'] + cases['METHODS']
)
# </editor-fold>

# <editor-fold desc="STEP #3 - Keyword screening of subsidiaries activities">

print('STEP #3 - Keyword screening of subsidiaries activities')

# Screen keywords in subsidiaries activity description fields and calculate subsidiary exposure
if not files['SUBS']['SCREENING_PATH'].exists():
    report = mtd.screen_subs(cases, files, keywords, subs_fin)

    r.write('Step #3 - Keyword screening of subsidiaries activities\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read listed companies subsidiaries - Screening.csv ...')

screen_subs = pd.read_csv(
    files['SUBS']['SCREENING_PATH'],
    na_values='n.a.',
    dtype={
        col: str for col in ['sub_bvd9', 'sub_bvd_id']
    },
    usecols=['sub_bvd9', 'sub_turnover_masked', 'sub_turnover']
)
# </editor-fold>

# <editor-fold desc="STEP #4 - Calculating group and subsidiary level exposure">

print('STEP #4 - Calculating group and subsidiary level exposure')

# Loading exposure at subsidiary and main company level
if not files['SUBS']['EXPOSURE_PATH'].exists():
    mtd.compute_sub_exposure(cases, files, select_subs, screen_subs)

print('# Read ' + str(company_type) + ' - exposure.csv ...')

main_comp_exposure = pd.read_csv(
    files['MAIN_COMPS']['EXPOSURE_PATH'],
    na_values='n.a.',
    dtype={
        col: str for col in ['bvd9']
    }
)

print('# Read ' + str(company_type) + ' subsidiaries - exposure.csv ...')

sub_exposure = pd.read_csv(
    files['SUBS']['EXPOSURE_PATH'],
    na_values='n.a.',
    dtype={
        col: str for col in ['bvd9', 'sub_bvd9']
    }
)

#
if not files['MAIN_COMPS']['RND_PATH'].exists():
    mtd.compute_main_comp_rnd(cases, files, main_comp_exposure, main_comp_fin)

print('# Read ' + str(company_type) + ' - rnd estimates.csv ...')

main_comp_rnd = pd.read_csv(
    files['MAIN_COMPS']['RND_PATH'],
    na_values='n.a.',
    dtype={
        col: str for col in ['bvd9']
    }
)

if not files['SUBS']['RND_PATH'].exists():
    mtd.compute_sub_rnd(cases, files, sub_exposure, main_comp_rnd)

print('# Read ' + str(company_type) + ' subsidiaries - rnd estimates.csv ...')

sub_rnd = pd.read_csv(
    files['SUBS']['RND_PATH'],
    na_values='n.a.',
    dtype={
        col: str for col in ['bvd9']
    }
)
# </editor-fold>

r.close()
