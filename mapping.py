# Import libraries
import pandas as pd
import method as mtd
import config as cfg
import report as rpt
import datetime
import json
from pathlib import Path

# Load config files
(cases, cases_as_strings, files, use_case, place) = cfg.init()

# Import country mapping tales
if not files['MAPPING']['COUNTRY_REFERENCE_PATH'].exists():
    mtd.create_country_map(cases, files)

# Import mapping tables
print('Read country_table.csv - Country mapping table ...')
country_map = pd.read_csv(files['MAPPING']['COUNTRY_REFERENCE_PATH'])

# Format SOEUR_RnD reference file
if not files['MAPPING']['SOEUR_RND_REFERENCE_PATH'].exists():
    print('Read SOEUR RnD source file')

    df = pd.read_excel(files['MAPPING']['SOEUR_RND_SOURCE_PATH'],
                       sheet_name='SOEUR_RnD',
                       names=['group_id', 'group_name', 'group_country_2DID_iso', 'id_group_region',
                              'group_region', 'is_group_MI', 'bvd_id', 'icb_id', 'icb_3_name', 'NACE_code',
                              'sector_uc', 'group_uc', 'rnd_group_uc', 'group_size', 'group_rnd',
                              'group_sales', 'group_employees', 'group_invention', 'group_energy_invention', 'year',
                              'jrc_id', 'company_name', 'sector', 'id_world_player',
                              'world_player', 'is_MI', 'country_order', 'country_2DID_iso', 'nuts1', 'nuts2', 'nuts3',
                              'total_invention', 'id_tech', 'technology', 'actions', 'energy_union_priority', 'tech_uc',
                              'invention', 'invention_granted', 'invention_high_value', 'invention_citation',
                              'rnd_final', 'equation'
                              ],
                       na_values='n.a.',
                       dtype={
                           **{col: str for col in
                              ['group_id', 'group_name', 'group_country_2DID_iso', 'year', 'company_name',
                               'country_2DID_iso', 'technology', 'actions', 'energy_union_priority'
                               ]},
                           **{col: float for col in
                              ['rnd_final']}
                       }
                       ).drop(
        columns=['id_group_region', 'group_region', 'is_group_MI', 'bvd_id', 'icb_id', 'icb_3_name', 'NACE_code',
                 'sector_uc', 'group_uc', 'rnd_group_uc', 'group_size', 'group_rnd', 'group_sales', 'group_employees',
                 'group_invention', 'group_energy_invention', 'jrc_id', 'sector', 'id_world_player', 'world_player',
                 'is_MI', 'country_order', 'nuts1', 'nuts2', 'nuts3', 'total_invention', 'id_tech', 'tech_uc',
                 'invention', 'invention_granted', 'invention_high_value', 'invention_citation', 'equation'],
    )

    df = df[df['energy_union_priority'] != 'z_Others']

    df['approach'] = files['MAPPING']['SOEUR_RND_VERSION']

    print('Merge with country mapping tables')

    # Merging group country_map for allocation to world player categories
    merged = pd.merge(
        df, country_map[['country_2DID_soeur', 'country_3DID_iso', 'world_player']],
        left_on='country_2DID_iso', right_on='country_2DID_soeur',
        how='left',
        suffixes=(False, False)
    )

    print('Save SOEUR RnD reference file')

    merged.to_csv(files['MAPPING']['SOEUR_RND_REFERENCE_PATH'],
                  index=False,
                  columns=['group_id', 'group_name', 'group_country_2DID_iso', 'year', 'company_name',
                           'country_2DID_iso', 'country_2DID_soeur', 'country_3DID_iso', 'world_player', 'technology',
                           'actions', 'energy_union_priority', 'rnd_final', 'approach'],
                  float_format='%.10f',
                  na_rep='n.a.'
                  )
