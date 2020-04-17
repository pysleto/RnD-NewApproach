# Import libraries
import os
import pandas as pd
from pathlib import Path
import configparser
import json
from tabulate import tabulate
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
