# Import libraries
import pandas as pd
import Input as input

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

# Select main companies by world region
file_path = config['CASE_ROOT'].joinpath(r'Listed companies.csv')

if not file_path.exists():
    input.select_main(config['CASE_ROOT'], config['YEAR_LASTAV'], config['REGION'], CountryMap)

print('Read list of selected listed companies ...')
SelectMain = pd.read_csv(config['CASE_ROOT'].joinpath(r'Listed companies.csv'))

# Load list of subsidiaries
file_path = config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries.csv')

if not file_path.exists():
    input.select_subs(config['CASE_ROOT'],config['SUBS_ID_FILE_N'])

print('Read list of corresponding subsidiaries ...')
SelectSubs = pd.read_csv(config['CASE_ROOT'].joinpath(r'Listed companies subsidiaries.csv'))
