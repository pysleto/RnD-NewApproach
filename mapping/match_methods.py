import pandas as pd

from tabulate import tabulate

import init_config as cfg

# Load config files
reg = cfg.load_my_registry()


# TODO: Directly import from last soeur DBB table
def consolidate_soeur_comps():
    # Read soeur group ids
    soeur_parents = pd.read_csv(
        reg['project_root'].joinpath(r'mapping\JRC004_MNC.csv'),
        na_values='#N/A',
        dtype=str
    )

    soeur_parents['is_soeur_group'] = True

    soeur_parents['soeur_parent_name'] = soeur_parents['soeur_group_name']
    soeur_parents['soeur_parent_id'] = soeur_parents['soeur_group_id']

    soeur_parents.rename(columns={'soeur_group_name': 'soeur_name'}, inplace=True)

    soeur_parents.drop_duplicates(subset=['soeur_name'], inplace=True)

    soeur_parents = soeur_parents[
        ['soeur_name', 'is_soeur_group', 'soeur_group_id', 'scoreboard_name', 'current_bvd_name', 'current_bvd_id',
         'soeur_parent_id', 'soeur_parent_name']]

    soeur_parents.soeur_parent_id.replace('0', '#N/A', inplace=True)

    soeur_parents.set_index('soeur_name', inplace=True)

    # Read soeur sub ids
    soeur_subs = pd.read_csv(
        reg['project_root'].joinpath(r'ref_tables/SOEUR_RnD_2019b/SOEUR_rnd_2019b_sub_ids.csv'),
        na_values='#N/A',
        dtype=str
    )

    soeur_subs['is_soeur_sub'] = True

    soeur_subs.rename(columns={'soeur_group_id': 'soeur_parent_id', 'soeur_group_name': 'soeur_parent_name',
                               'soeur_sub_name': 'soeur_name'}, inplace=True)

    soeur_subs.drop_duplicates(subset=['soeur_name'], inplace=True)

    soeur_subs = soeur_subs[
        ['soeur_name', 'is_soeur_sub', 'soeur_sub_id', 'soeur_parent_id', 'soeur_parent_name']]

    soeur_subs.soeur_parent_id.replace('0', '#N/A', inplace=True)

    soeur_subs.set_index('soeur_name', inplace=True)

    # Consolidate list of soeur companies
    soeur_comps = pd.merge(
        soeur_parents,
        soeur_subs,
        left_index=True, right_index=True,
        how='outer',
        suffixes=('_parent', '_sub')

    )

    soeur_comps.is_soeur_group.fillna(False, inplace=True)
    soeur_comps.is_soeur_sub.fillna(False, inplace=True)

    soeur_comps.loc[soeur_comps.is_soeur_sub == True, 'soeur_parent_name'] = soeur_comps['soeur_parent_name_sub']
    soeur_comps.loc[soeur_comps.is_soeur_sub == True, 'soeur_parent_id'] = soeur_comps.soeur_parent_id_sub

    soeur_comps.loc[soeur_comps.is_soeur_group == True, 'soeur_parent_name'] = soeur_comps.soeur_parent_name_parent
    soeur_comps.loc[soeur_comps.is_soeur_group == True, 'soeur_parent_id'] = soeur_comps.soeur_parent_id_parent

    soeur_comps = soeur_comps[
        ['is_soeur_group', 'soeur_group_id', 'is_soeur_sub', 'soeur_sub_id', 'scoreboard_name', 'current_bvd_name',
         'current_bvd_id', 'soeur_parent_id', 'soeur_parent_name']]

    soeur_comps[['soeur_group_id', 'soeur_sub_id', 'current_bvd_id', 'soeur_parent_id']].astype('str', copy=False)

    soeur_comps.to_csv(
        reg['project_root'].joinpath(r'ref_tables/SOEUR_RnD_2019b/SOEUR_rnd_2019b_comp_ids.csv'),
        na_rep='#N/A')

    return soeur_comps


def update_current_match(input_count, update, file_path, match_dtypes):

    print(
        tabulate([[
            'Input: ' + str(input_count),
            'Table update: ' + str(update.index.value_counts().sum())
        ]], tablefmt="plain")
    )

    # print(table_update[~table_update['step'].isna()].head())

    # print('Update working table')

    current_match = load_current_match_csv(file_path, match_dtypes)

    current_match.set_index(['soeur_name', 'soeur_country_2DID_iso'], inplace=True)

    # print('... Update with newly matched data')

    current_match.update(update)

    # print(working_table.head())

    # print('... Save to csv')

    # current_match.astype('str', copy=False)

    current_match.to_csv(
        file_path,
        na_rep='#N/A'
    )

    return current_match


def load_current_match_csv(file_path, match_dtypes):

    current_match = pd.read_csv(
        file_path,
        dtype=match_dtypes
    )

    return current_match