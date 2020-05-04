# Import libraries
import os
from pathlib import Path

import pandas as pd

from ref_tables import ref_methods as mtd

# Set  dataframe display options
pd.options.display.max_columns = None
pd.options.display.width = None

root = Path(os.getcwd())

soeur_version = 'SOEUR_rnd_2019b'

soeur_path = Path(r'C:\Users\Simon\PycharmProjects\rnd-private\data_input\soeur_rnd\SOEUR_rnd_2019b_20200309.xlsx')

soeur_rnd = mtd.update_n_format_soeur_rnd(soeur_path)

print('Save output table')
print('... at subsidiary level')

# Save output tables at subsidiary level

# TODO: Clean the names of subsidiaries from patstat
# soeur subs identification tables
subs_ids = soeur_rnd[['soeur_group_id', 'soeur_group_name', 'is_embedded_in_MNC', 'soeur_sub_id', 'soeur_sub_name',
                      'sub_country_2DID_iso', 'sub_NUTS1', 'sub_NUTS2', 'sub_NUTS3']].drop_duplicates().dropna(
    subset=['soeur_sub_id'])

subs_ids.to_csv(root.joinpath(soeur_version + '_sub_ids.csv'),
                index=False,
                na_rep='#N/A'
                )

sub_rnd_cols = ['soeur_sub_id', 'soeur_sub_name', 'is_embedded_in_MNC', 'sub_country_2DID_iso',
                'sub_country_3DID_iso', 'sub_world_player', 'year', 'rnd_clean']

soeur_rnd.to_csv(root.joinpath(soeur_version + '_by_sub.csv'),
                 columns=sub_rnd_cols,
                 float_format='%.10f',
                 index=False,
                 na_rep='#N/A'
                 )

print('... at group level')

# Save output tables at group level
group_rnd_cols = ['soeur_group_id', 'soeur_group_name', 'is_embedded_in_MNC', 'group_country_2DID_iso',
                  'group_country_3DID_iso', 'group_world_player', 'year', 'sector_UC', 'group_UC', 'rnd_group_UC',
                  'rnd_clean']

# soeur group identification tables
group_ids = soeur_rnd[['soeur_group_id', 'soeur_group_name', 'soeur_group_bvd_id', 'is_embedded_in_MNC',
                       'group_country_2DID_iso']].drop_duplicates().dropna(subset=['soeur_group_name'])

group_ids.to_csv(root.joinpath(soeur_version + '_group_ids.csv'),
                 index=False,
                 na_rep='#N/A'
                 )

# soeur group exposure and rnd estimates
soeur_rnd_by_group = soeur_rnd.groupby(group_rnd_cols[:7]).agg({
    **{col: 'mean' for col in ['sector_UC', 'group_UC', 'rnd_group_UC']},
    'rnd_clean': 'sum'
})

soeur_rnd_by_group.reset_index(inplace=True)

soeur_rnd_by_group.to_csv(root.joinpath(soeur_version + '_by_group.csv'),
                          columns=group_rnd_cols,
                          float_format='%.10f',
                          index=False,
                          na_rep='#N/A'
                          )

print('... at region level')

regtech_rnd_cols = ['year', 'sub_country_2DID_iso', 'sub_country_3DID_iso', 'sub_world_player', 'is_embedded_in_MNC',
                    'technology', 'action', 'priority', 'rnd_clean']

soeur_rnd_by_region_n_tech = soeur_rnd.groupby(regtech_rnd_cols[:-1]).sum()

soeur_rnd_by_region_n_tech.reset_index(inplace=True)

soeur_rnd_by_region_n_tech.to_csv(root.joinpath(soeur_version + '_by_region_n_tech.csv'),
                                  columns=regtech_rnd_cols,
                                  float_format='%.10f',
                                  index=False,
                                  na_rep='#N/A'
                                  )


