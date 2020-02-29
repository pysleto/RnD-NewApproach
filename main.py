# Import libraries
import pandas as pd
import methods as mtd
from tabulate import tabulate
import datetime

# <editor-fold desc="STEP #0 - Initialisation">

print('STEP #0 - Initialisation')

# Set initial parameters
case = 'EU_28'
place = 'home'
company_type = 'Listed companies'
base = r'C:\Users\letousi\PyProjs\RnD-NewApproach'
data = r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020'

if place == 'home':
    base = r'C:\Users\Simon\Documents\PyProjs\RnD-NewApproach'
    data = base

print('Load configuration parameters ...')

# Load config file
config = mtd.import_my_config(case, base, data)

# Initialize report file
report_path = config['CASE_ROOT'].joinpath(r'Report.txt')

if report_path.exists():
    r = open(report_path, 'a')
else:
    r = open(report_path, 'w')
    r.write('Step #0 - Initialisation\n\n')
    r.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ '\n\n')
    r.write('Place = ' + place + '\n' + 'Type = ' + company_type + '\n\n')

    for key, value in config.items():
        r.write(key + ' = ' + str(value) + '\n')
#    pprint(config, stream=r)
    r.write('\n')

# Import mapping tales
file_path = config['CASE_ROOT'].joinpath(r'Mapping\Country_table.csv')

if not file_path.exists():
    mtd.create_country_map(config['MAPPING'], config['CASE_ROOT'])

print('Read country mapping table ...')
CountryMap = pd.read_csv(config['CASE_ROOT'].joinpath(r'Mapping\Country_table.csv'))
# </editor-fold>

# <editor-fold desc="STEP #1 - Selection of main companies">

print('STEP #1 - Selection of main companies')

# Select main companies by world region
file_path = config['CASE_ROOT'].joinpath(r'Listed companies.csv')

if not file_path.exists():
    report = mtd.select_main(config['CASE_ROOT'], config['YEAR_LASTAV'], config['REGIONS'], CountryMap)

    r.write('Step #1 - Selection of main companies\n\n')
    r.write(str(config['YEAR_LASTAV']) + ' ¦ ' + company_type + ' ¦ ' + 'RnD in EUR million\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read list of selected listed companies ...')
SelectMain = pd.read_csv(
    config['CASE_ROOT'].joinpath(r'Listed companies.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['BvD9', 'BvD_id']
    }
)
# </editor-fold>

# <editor-fold desc="STEP #2 - Consolidation of subsidiaries and main companies">

print('STEP #2 - Consolidation of subsidiaries and main companies')

# Load list of subsidiaries
file_path = config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries.csv')

if not file_path.exists():
    report = mtd.select_subs(config['CASE_ROOT'], config['SUBS_ID_FILE_N'])

    r.write('Step #2 - Consolidation of subsidiaries\n\n')
    r.write(tabulate(report, tablefmt='simple', headers=report.columns))
    r.write('\n\n')

print('Read list of corresponding subsidiaries ...')
SelectSubs = pd.read_csv(
    config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['BvD9', 'BvD_id', 'Sub_BvD9', 'Sub_BvD_id']
    }
)
# </editor-fold>

r.close()
