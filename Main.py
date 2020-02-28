# Import libraries
import pandas as pd
import Input as input

# <editor-fold desc="STEP #0 - Initialisation">

print('STEP #0 - Initialisation')

# Set configuration parameters
case = 'EU_28'
base = r'C:\Users\letousi\PyProjs\RnD-NewApproach'
data = r'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020'

config = input.import_my_config(case, base, data)

# Import mapping tales
file_path = config['CASE_ROOT'].joinpath(r'Mapping\Country_table.csv')

if not file_path.exists():
    input.create_country_map(config['MAPPING'], config['CASE_ROOT'])

print('Read country mapping table ...')
CountryMap = pd.read_csv(config['CASE_ROOT'].joinpath(r'Mapping\Country_table.csv'))
# </editor-fold>

# <editor-fold desc="STEP #1 - Selection of main companies">

print('STEP #1 - Selection of main companies')

# Select main companies by world region
file_path = config['CASE_ROOT'].joinpath(r'Listed companies.csv')

if not file_path.exists():
    input.select_main(config['CASE_ROOT'], config['YEAR_LASTAV'], config['REGION'], CountryMap)

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
    input.select_subs(config['CASE_ROOT'],config['SUBS_ID_FILE_N'])

print('Read list of corresponding subsidiaries ...')
SelectSubs = pd.read_csv(
    config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries.csv'),
    na_values='n.a.',
    dtype={
        col: str for col in ['BvD9', 'BvD_id', 'Sub_BvD9', 'Sub_BvD_id']
    }
)
# </editor-fold>
