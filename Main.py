# Import libraries
import os
import pandas as pd
from pathlib import Path
import configparser
import json
import Input as input

# Set configuration parameters
case = 'EU_28'
base = 'U:\WP 765 Energy RIC\Private data & analysis\Alternative Approach_Private R&D\Orbis_Data\Data_2020'

config = input.import_my_config(case, base)

# Import mapping tales
file_path = config['CASE_ROOT'].joinpath(r'Mapping\Country_table.csv')

if not file_path.exists():
    input.create_country_map(config['MAPPING'], config['CASE_ROOT'])

print('Read country mapping table ...')
CountryMap = pd.read_csv(config['CASE_ROOT'].joinpath(r'Mapping\Country_table.csv'))

# Select main companies by world region
file_path = config['CASE_ROOT'].joinpath(r'Listed companies.csv')

if not file_path.exists():
    report = input.select_main(config['CASE_ROOT'], config['YEAR_LASTAV'], config['REGION'], CountryMap)

print('Read list of selected listed companies ...')
SelectMain = pd.read_csv(config['CASE_ROOT'].joinpath(r'Listed companies.csv'))
