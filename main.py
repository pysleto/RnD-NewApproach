# Import libraries
import pandas as pd
import methods as mtd
from tabulate import tabulate
import datetime
import json

# <editor-fold desc="STEP #0 - Initialisation">

print('STEP #0 - Initialisation')

# Set initial parameters
case = 'EU_28'
place = 'home'
company_type = 'Listed companies'
base = r'C:\Users\letousi\PycharmProjects\RnD-NewApproach'
data = r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020'

if place == 'home':
    base = r'C:\Users\Simon\Documents\PyProjs\RnD-NewApproach'
    data = base

print('Read config.ini - Configuration parameters ...')

# Load config file
config = mtd.import_my_config(case, base, data)

# Initialize file paths
report_path = config['CASE_ROOT'].joinpath(r'Report.txt')
map_path = config['CASE_ROOT'].joinpath(r'Mapping\Country_table.csv')
comps_path = config['CASE_ROOT'].joinpath(r'Listed companies.csv')
comps_fin_path = config['CASE_ROOT'].joinpath(r'Listed companies - financials.csv')
subs_path = config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries.csv')
subs_path_w_filters = config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - methods.csv')
subs_fin_path = config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - financials.csv')
subs_screen_path = config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - screening.csv')

if report_path.exists():
    r = open(report_path, 'a')
else:
    r = open(report_path, 'w')
    r.write('Step #0 - Initialisation\n\n')
    r.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '\n\n')
    r.write('Place = ' + place + '\n' + 'Type = ' + company_type + '\n\n')

    for key, value in config.items():
        r.write(key + ' = ' + str(value) + '\n')
    #    pprint(config, stream=r)
    r.write('\n')

# Import mapping tales
if not map_path.exists():
    mtd.create_country_map(config['MAPPING'], config['CASE_ROOT'])

print('Read country_table.csv - Country mapping table ...')
country_map = pd.read_csv(config['CASE_ROOT'].joinpath(r'Mapping\Country_table.csv'))
# </editor-fold>

# <editor-fold desc="STEP #1 - Selection of main companies">

print('STEP #1 - Selection of main companies and consolidation of their financials')

# Select main companies by world region
if not comps_path.exists():
    report = mtd.select_main(config['CASE_ROOT'], config['YEAR_LASTAV'], config['REGIONS'], country_map)

    r.write('Step #1 - Selection of main companies\n\n')
    r.write(str(config['YEAR_LASTAV']) + ' ¦ ' + company_type + ' ¦ ' + 'RnD in EUR million\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read selected listed companies ...')
select_comps = pd.read_csv(
    config['CASE_ROOT'].joinpath(r'Listed companies.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['BvD9', 'BvD_id']
    }
)

# Load main companies financials
if not comps_fin_path.exists():
    report = mtd.load_main_comps_fin(config['CASE_ROOT'], config['YEAR_LASTAV'], config['MAIN_COMPS_FIN_FILE_N'],
                                     select_comps)

    r.write('Companies with financials and declared RnD\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read selected main companies financials ...')
main_comps_fin = pd.read_csv(
    config['CASE_ROOT'].joinpath(r'Listed companies - financials.csv'),
    na_values='n.a.',
    usecols=['BvD9', 'Company_name'] + ['RnD_Y' + str(YY) for YY in range(10, 20)[::-1]],
    dtype={
        col: str for col in ['BvD9']
    }
)
# </editor-fold>

# <editor-fold desc="STEP #2 - Consolidation of subsidiaries and main companies">

print('STEP #2 - Consolidation of subsidiaries and main companies')

# Load list of subsidiaries
if not subs_path.exists():
    report = mtd.select_subs(config['CASE_ROOT'], config['SUBS_ID_FILE_N'])

    r.write('Step #2 - Consolidation of subsidiaries from selected main companies\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read Listed companies subsidiaries.csv ...')
select_subs = pd.read_csv(
    config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['BvD9', 'BvD_id', 'Sub_BvD9', 'Sub_BvD_id']
    }
)

# Load subsidiaries financials
if not subs_fin_path.exists():
    report = mtd.load_subs_fin(config['CASE_ROOT'], config['SUBS_FIN_FILE_N'], select_subs, country_map)

    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read Listed companies subsidiaries - financials.csv ...')
subs_fin = pd.read_csv(
    config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - financials.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['Sub_BvD9', 'Sub_BvD_id']
    }
)

# Analyze main companies and subsidiaries
if not subs_path_w_filters.exists():
    report = mtd.filter_comps_and_subs(config['CASE_ROOT'], select_subs, subs_fin)

    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read Listed companies subsidiaries - methods.csv ...')

select_subs = pd.read_csv(
    config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - methods.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['Sub_BvD9', 'Sub_BvD_id']
    },
    usecols=['Company_name', 'BvD9', 'Sub_company_name','Sub_BvD9', 'keep_all', 'keep_comps', 'keep_subs']
)
# </editor-fold>

# <editor-fold desc="STEP #3 - Keyword screening of subsidiaries activities">

print('STEP #3 - Keyword screening of subsidiaries activities')

with open(config['BASE'].joinpath(r'Keywords.json'), 'r') as file:
    keywords = json.load(file)

# Screen keywords in subsidiaries activity description fields and calculate subsidiary exposure
if not subs_screen_path.exists():
    report = mtd.screen_subs(config['CASE_ROOT'], keywords, subs_fin)

    r.write('Step #3 - Keyword screening of subsidiaries activities\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read Listed companies subsidiaries - Screening.csv ...')

screen_subs = pd.read_csv(
    config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - screening.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['Sub_BvD9', 'Sub_BvD_id']
    },
    usecols=['Sub_BvD9', 'Sub_turnover_masked', 'Sub_turnover']
)
# </editor-fold>

# <editor-fold desc="STEP #4 - Calculating group and subsidiary level exposure">

print('STEP #4 - Calculating group and subsidiary level exposure')

for method in config['METHODS']:
    print('Calculating exposures for method: ' + method + ' ...')

    sub_exposure_path = config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - exposure - ' + method + '.csv')
    main_comp_RnD_path = config['CASE_ROOT'].joinpath(r'Listed companies - RnD estimates- ' + method + '.csv')
    sub_RnD_path = config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - RnD estimates- ' + method + '.csv')

    #
    if not sub_exposure_path.exists():
        mtd.compute_sub_exposure(config['CASE_ROOT'], select_subs, screen_subs, method)

    print('Read Listed companies and subsidiaries - exposure - ' + method + '.csv ...')

    main_comp_exposure = pd.read_csv(
        config['CASE_ROOT'].joinpath(r'Listed companies - exposure - ' + method + '.csv'),
        na_values='n.a.',
        dtype={
            col: str for col in ['BvD9']
        }
    )

    sub_exposure = pd.read_csv(
        config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - exposure - ' + method + '.csv'),
        na_values='n.a.',
        dtype={
            col: str for col in ['BvD9', 'Sub_BvD9']
        }
    )

    #
    if not main_comp_RnD_path.exists():
        mtd.compute_main_comp_RnD(config['CASE_ROOT'], main_comp_exposure, main_comps_fin, method)

    print('Read Listed companies - RnD estimates - ' + method + '.csv ...')

    main_comp_RnD = pd.read_csv(
        config['CASE_ROOT'].joinpath(r'Listed companies - RnD estimates - ' + method + '.csv'),
        na_values='n.a.',
        dtype={
            col: str for col in ['BvD9']
        }
    )

    if not sub_RnD_path.exists():
        mtd.compute_sub_RnD(config['CASE_ROOT'], sub_exposure, main_comp_RnD, method)

    print('Read Listed companies subsidiaries - RnD estimates- ' + method + '.csv ...')

    sub_RnD = pd.read_csv(
        config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries - RnD estimates - ' + method + '.csv'),
        na_values='n.a.',
        dtype={
            col: str for col in ['BvD9']
        }
    )
# </editor-fold>

r.close()
