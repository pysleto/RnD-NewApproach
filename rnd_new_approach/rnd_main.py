# Import libraries
import sys

from config import registry as reg
from config import col_ids as col

import pandas as pd
import datetime
import json

from rnd_new_approach import rnd_methods as rd_mtd
from benchmark import by_methods as by_mtd

# TODO: de couple ORBIS parent and subs consolidation from rnd
# TODO: import previous name to integrate name change in fuzzy match
# TODO: Abstract main.py with object class and functions for patterns

# <editor-fold desc="#0 - Initialisation">
print('#0 - Initialisation')

# TODO: Clean reporting
# TODO: Conso_scope as a registry key
conso_scope = ['C1', 'C2', 'C*', 'U1', 'U*', 'LF', 'NF']
# conso_scope = ['C1', 'C2', 'C*', 'U1', 'U2', 'U*', 'LF', 'NF']

# TODO: Save a digest of config parameters in the data folder
# Initialize report
# report = {}
#
# if reg.case_path.joinpath(r'report.json').exists():
#     # Load existing file
#     with open(reg.case_path.joinpath(r'report.json'), 'r') as file:
#         report = json.load(file)
#
#     # Update time stamp
#     report['initialisation']['Datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# else:
#     reg_to_str = {}
#
#     for key in reg.keys():
#         reg_to_str[key] = str(reg[key])
#
#     report['initialisation'] = {
#         'Datetime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#         'Use case': reg_to_str
#     }
#
# rd_mtd.update_report(report)
# # </editor-fold>

# <editor-fold desc="#0 - Consolidate a full list of parent companies bvd9">
print('#0 - Consolidate parent companies scope')

# Select parent companies
if not reg.conso_bvd9_full_path.exists():
    conso_ids = rd_mtd.load_parent_ids('initial', conso_ids=pd.DataFrame())
    # rd_mtd.update_report(report)

    print('Save output file - parent_ids initial scope consolidated ... ')

    conso_ids.to_csv(reg.conso_bvd9_full_path,
                     columns=col.conso_ids,
                     float_format='%.10f',
                     index=False,
                     na_rep='#N/A'
                     )

print('Read file - conso_ids table ... ')

conso_ids = pd.read_csv(
    reg.conso_bvd9_full_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('Consolidated count')

print('parent_bvd9:' + str(pd.Series(conso_ids.loc[conso_ids['is_parent'] == True, 'bvd9'].unique()).count()))
print('guo_bvd9:' + str(pd.Series(conso_ids.loc[conso_ids['is_GUO'] == True, 'bvd9'].unique()).count()))
print('all_bvd9:' + str(pd.Series(conso_ids.bvd9.unique()).count()))
# </editor-fold>

# <editor-fold desc="#1 - Select parent companies">
print('#1 - Select parent companies')

# report['select_parents'] = {}

# TODO: Define global heads and consolidate global head parents tuples
# Select parent companies
if not (reg.parent_id_path.exists() & reg.guo_id_path.exists()):
    # (report['select_parents'], parent_ids, guo_ids) = rd_mtd.load_parent_ids()
    (parent_ids, guo_ids) = rd_mtd.load_parent_ids('consolidated', conso_ids)
    # rd_mtd.update_report(report)

    print('Save output file - parent_ids table ... ')

    parent_ids = by_mtd.select_by_account(parent_ids, 'parent', conso_scope)

    parent_ids.to_csv(reg.parent_id_path,
                      columns=col.parent_ids + ['country_3DID_iso', 'world_player'],
                      float_format='%.10f',
                      index=False,
                      na_rep='#N/A'
                      )

    print('Save output file - guo_ids table ...')

    guo_ids = by_mtd.select_by_account(guo_ids, 'guo', conso_scope)

    guo_ids.to_csv(reg.guo_id_path,
                   columns=col.guo_ids,
                   float_format='%.10f',
                   index=False,
                   na_rep='#N/A'
                   )

print('Read file - parent_ids table ... ')

parent_ids = pd.read_csv(
    reg.parent_id_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('bvd9_in_parent_ids:' + str(pd.Series(parent_ids.bvd9.unique()).count()))

print('Read file - guo_ids table ... ')

guo_ids = pd.read_csv(
    reg.guo_id_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('guo_bvd9_in_guo_ids:' + str(pd.Series(guo_ids.guo_bvd9.unique()).count()))
# </editor-fold>

# TODO: collect a simple list of sub_bvd9
# TODO: Consolidate a conso_ids of unique (guos, parents, subs, consolidation, country) tuples
# <editor-fold desc="#2a - Identify and collect subsidiary">
print('#2a - Identify and collect subsidiary')

if not reg.sub_bvd9_full_path.exists():
    # (report['load_subsidiary_identification'], sub_ids) = rd_mtd.load_sub_ids()
    sub_ids_collect = rd_mtd.load_sub_ids()

    print('Save output file - sub_bvd9_ids table ... ')

    # Save lists of subsidiary bvd9 ids
    sub_bvd9_ids = pd.Series(sub_ids_collect.sub_bvd9.unique())

    sub_bvd9_ids.to_csv(reg.sub_bvd9_full_path,
                        index=False,
                        header=False,
                        na_rep='#N/A'
                        )

print('Read file - sub_bvd9_ids table ... ')

sub_bvd9_ids = pd.read_csv(
    reg.sub_bvd9_full_path,
    na_values='#N/A',
    dtype=col.dtype
)
# </editor-fold>

# TODO: collect further identificiation data aligned with parent_identification, screen by account and consolidate a subs_id table for each above tuple
# TODO: select by accounts
# <editor-fold desc="#2b - Load subsidiary identification and flag for calculation methods">
print('#2b - Load subsidiary identification')

if not reg.sub_id_path.exists():
    # (report['load_subsidiary_identification'], sub_ids) = rd_mtd.load_sub_ids()
    sub_ids = rd_mtd.load_sub_ids()

    # selected_sub_ids = sub_ids[sub_ids.bvd9.isin(selected_parent_bvd9_ids)]

    sub_ids = rd_mtd.screen_sub_ids_for_method(parent_ids, sub_ids)
    # (report['screen_subsidiaries_for_method'], sub_ids) = rd_mtd.screen_sub_ids_for_method(parent_ids, sub_ids)
    #
    # rd_mtd.update_report(report)

    # selected_sub_bvd9_ids = pd.Series(selected_sub_ids.sub_bvd9.unique())
    #
    # selected_sub_bvd9_ids.to_csv(reg.sub_bvd9_short_path,
    #                              index=False,
    #                              header=False,
    #                              na_rep='#N/A'
    #                              )

    # Update retrieved subsidiary count in parent_ids
    # if 'subs_n_collected' not in parent_id_cols:
    #     parent_id_cols.insert(parent_id_cols.index('subs_n') + 1, 'subs_n_collected')
    #
    #     parent_ids = pd.merge(
    #         parent_ids,
    #         sub_ids[['bvd9', 'sub_bvd9']].groupby(['bvd9']).count().rename(
    #             columns={'sub_bvd9': 'subs_n_collected'}
    #         ).reset_index(),
    #         left_on='bvd9', right_on='bvd9',
    #         how='left',
    #         suffixes=(False, False)
    #     )

    print('Save output file - sub_ids table ... ')

    # TODO: Check for subsidiaries that have the same name but several corresponding bvd9 ids
    # Save it as csv
    sub_ids.to_csv(reg.sub_id_path,
                   columns=col.sub_ids,
                   float_format='%.10f',
                   index=False,
                   na_rep='#N/A'
                   )

print('Read file - sub_ids table ... ')

sub_ids = pd.read_csv(
    reg.sub_id_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('sub_bvd9_in_sub_ids:' + str(pd.Series(sub_ids.sub_bvd9.unique()).count()))
#     selected_sub_bvd9_ids = pd.read_csv(reg.sub_bvd9_short_path,
#                                         na_values='#N/A',
#                                         header=None,
#                                         dtype=str
#                                         )[0]
#
#     selected_sub_ids = sub_ids[sub_ids.sub_bvd9.isin(selected_sub_bvd9_ids)]
#
# selected_sub_id_cols = list(selected_sub_ids.columns)
# </editor-fold>

# TODO: Move after subsidiary identification to consolidate a conso_fins of unique (guos, parents, subs, consolidation, country) tuples
# TODO: Collect global heads and subsidiaries financials over the same format
# <editor-fold desc="#3 - Load parent company financials">
print('#3 - Load parent company financials')

if not reg.parent_fin_path.exists():
    parent_fins = rd_mtd.load_parent_fins()

    parent_fins.sort_values(by=['oprev_sum', 'rnd_sum'], ascending=False, na_position='last', inplace=True)

    print('Save output file - parent_fins table ... ')

    parent_fins = by_mtd.select_by_account(parent_fins, 'parent', conso_scope)

    # Save it as csv
    parent_fins.to_csv(reg.parent_fin_path,
                       columns=col.parent_fins,
                       float_format='%.10f',
                       index=False,
                       na_rep='#N/A'
                       )

    # (report['load_parent_financials'], parent_fins) = rd_mtd.load_parent_fins()

    # TODO: Check that selected parent ids based on rnd_limit is representative of total rnd in each world region
    # selected_parent_ids = rd.select_parent_ids_with_rnd(parent_fins)
    #
    # selected_parent_bvd9_ids = pd.Series(selected_parent_ids.bvd9.unique())
    #
    # selected_parent_bvd9_ids.to_csv(reg.parent_bvd9_short_path,
    #                                 float_format='%.10f',
    #                                 index=False,
    #                                 header=False,
    #                                 na_rep='#N/A'
    #                                 )

    # select = parent_fins[parent_fins['bvd9'].isin(parent_ids['bvd9'])]
    #
    # report['load_parent_financials']['With financials'] = {
    #     'total_bvd9': parent_fins['bvd9'].nunique(),
    #     'total_rnd_y' + str(reg.last_year)[-2:]: parent_fins['rnd_y' + str(reg.last_year)[-2:]].sum(),
    #     'selected_bvd9': select['bvd9'].nunique(),
    #     'selected_rnd_y' + str(reg.last_year)[-2:]: select['rnd_y' + str(reg.last_year)[-2:]].sum()
    # }

    # rd_mtd.update_report(report)

print('Read file - parent_fins table ... ')

parent_fins = pd.read_csv(
    reg.parent_fin_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('bvd9_in_parent_fins:' + str(pd.Series(parent_fins.bvd9.unique()).count()))

# selected_parent_bvd9_ids = pd.read_csv(
#     reg.parent_bvd9_short_path,
#     na_values='#N/A',
#     header=None,
#     dtype=str
# )[0]
# </editor-fold>

# <editor-fold desc="#4 - Load subsidiary financials and screen keywords in activity">
print('#4 - Load subsidiary financials')

if not reg.sub_fin_path.exists():
    sub_fins = rd_mtd.load_sub_fins()

    sub_fins.sort_values(by=['oprev_sum', 'rnd_sum'], ascending=False, na_position='last', inplace=True)

    sub_fins = by_mtd.select_by_account(sub_fins, 'sub', conso_scope)

    sub_fins = rd_mtd.screen_sub_fins_for_keywords(sub_fins)
    # (report['load_subsidiary_financials'], sub_fins) = rd_mtd.load_sub_fins()
    # (report['screen_subsidiary_activities'], sub_fins) = rd_mtd.screen_sub_fins_for_keywords(sub_fins)
    #
    # rd_mtd.update_report(report)

    print('Save output file - sub_fins table ... ')

    # Save it as csv
    sub_fins.to_csv(reg.sub_fin_path,
                    columns=col.sub_fins,
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

print('Read file - sub_fins table ... ')

sub_fins = pd.read_csv(
    reg.sub_fin_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('sub_bvd9_in_sub_fins:' + str(pd.Series(sub_fins.sub_bvd9.unique()).count()))
# </editor-fold>

# TODO: Move after conso_fins and conslidate financials (R&D and turnover) by global heads
# <editor-fold desc="#5 - Identify top R&D investing guos">
print('#5 - Identify top R&D investing guos')

if not 'rnd_sum' in guo_ids.keys():

    print('Update guo_ids with top rnd')

    # Merge with guo_id
    guo_rnd = pd.merge(
        parent_ids[['bvd9', 'guo_bvd9']],
        parent_fins[['bvd9', 'rnd_sum']],
        left_on='bvd9', right_on='bvd9',
        how='left',
        suffixes=(False, False)
    )

    guo_rnd_grouped = guo_rnd[['guo_bvd9', 'rnd_sum']].groupby(['guo_bvd9']).sum()

    guo_rnd_grouped.reset_index(inplace=True)

    guo_ids = pd.merge(
        guo_ids,
        guo_rnd_grouped[['guo_bvd9', 'rnd_sum']],
        left_on='guo_bvd9', right_on='guo_bvd9',
        how='left',
        suffixes=(False, False)
    )

    # TODO: Include the R&D disclosed by subsidiaries?
    guo_ids['is_top_2000'] = guo_ids['guo_bvd9'].isin(guo_ids.nlargest(2000, ['rnd_sum'])['guo_bvd9'])
    guo_ids['is_top_100'] = guo_ids['guo_bvd9'].isin(guo_ids.nlargest(100, ['rnd_sum'])['guo_bvd9'])

    guo_ids.dropna(subset=['guo_bvd9'], inplace=True)

    print('Update output file - guo_ids table ... ')

    # Save it as csv
    guo_ids.to_csv(reg.guo_id_path,
                   columns=col.guo_ids + ['is_top_2000', 'is_top_100', 'rnd_sum'],
                   float_format='%.10f',
                   index=False,
                   na_rep='#N/A'
                   )

print('Read file - guo_ids table ... ')

guo_ids = pd.read_csv(
    reg.guo_id_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('guo_bvd9_in_guo_ids_update:' + str(pd.Series(guo_ids.guo_bvd9.unique()).count()))
# </editor-fold>

# <editor-fold desc="#6 - Calculating group and subsidiary level exposure">
print('#6 - Calculating group and subsidiary level exposure')

# TODO: integrate parents that do not have subsidiaries (therefore are not managed by keep_sub) in exposure and rnd calculations
# Loading exposure at subsidiary and parent company level
if not (reg.parent_expo_path.exists() & reg.sub_expo_path.exists()):
    (parent_exposure, sub_exposure) = rd_mtd.compute_exposure(
        parent_ids,
        parent_fins,
        sub_ids,
        sub_fins
    )
    # (report['keyword_screen_by_method'], report['compute_exposure'], parent_exposure, sub_exposure) = \
    #     rd_mtd.compute_exposure(sub_ids,
    #                          sub_fins
    #                          )
    #
    # rd_mtd.update_report(report)

    print('Save output file - parent_expo table ... ')

    # Save output tables
    parent_exposure.to_csv(reg.parent_expo_path,
                           columns=col.parent_exp,
                           float_format='%.10f',
                           index=False,
                           na_rep='#N/A'
                           )

    print('Save output file - sub_expo table ... ')

    sub_exposure.to_csv(reg.sub_expo_path,
                        columns=col.sub_exp,
                        float_format='%.10f',
                        index=False,
                        na_rep='#N/A'
                        )

print('Read file - parent_expo table ... ')

print('bvd9_in_parent_expo:' + str(pd.Series(parent_exposure.bvd9.unique()).count()))

print('bvd9_in_parent_expo that are not N/A:' + str(
    pd.Series(parent_exposure.loc[~parent_exposure.parent_exposure.isna(), 'bvd9'].unique()).count()))

parent_exposure = pd.read_csv(
    reg.parent_expo_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('Read file - sub_expo table ... ')

print('sub_bvd9_in_sub_expo that are not N/A:' + str(
    pd.Series(sub_exposure.loc[~sub_exposure.sub_exposure.isna(), 'sub_bvd9'].unique()).count()))

print('sub_bvd9_in_sub_expo:' + str(pd.Series(sub_exposure.sub_bvd9.unique()).count()))

sub_exposure = pd.read_csv(
    reg.sub_expo_path,
    na_values='#N/A',
    dtype=col.dtype
)
# </editor-fold>

sys.exit()

# <editor-fold desc="#7a - Calculating parent level clean rnd">
print('#7a - Calculating parent level clean rnd')

if not reg.parent_rnd_path.exists():
    # report['compute_rnd'] = {}

    parent_rnd = rd_mtd.compute_parent_rnd(
        parent_exposure,
        parent_fins
    )

    # (report['compute_rnd']['at_parent_level'], parent_rnd) = rd_mtd.compute_parent_rnd(
    #     parent_exposure,
    #     parent_fins
    # )
    #
    # rd_mtd.update_report(report)

    print('Save output file - parent_rnd table ... ')

    parent_rnd.to_csv(reg.parent_rnd_path,
                      columns=col.parent_rnd,
                      float_format='%.10f',
                      index=False,
                      na_rep='#N/A'
                      )

print('Read file - parent_rnd table ... ')

parent_rnd = pd.read_csv(
    reg.parent_rnd_path,
    na_values='#N/A',
    dtype=col.dtype
)

print('bvd9_in_parent_rnd:' + str(pd.Series(parent_rnd.bvd9.unique()).count()))

print('bvd9_in_parent_rnd that are not N/A:' + str(
    pd.Series(parent_rnd.loc[~parent_rnd.parent_rnd.isna(), 'bvd9'].unique()).count()))
# </editor-fold>



sub_rnd = rd_mtd.compute_sub_rnd(sub_exposure, parent_rnd)

# (report['compute_rnd']['at_subsidiary_level'], sub_rnd) = rd_mtd.compute_sub_rnd(sub_exposure,
#                                                                               parent_rnd)
#
# rd_mtd.update_report(report)

# sub_rnd.drop_duplicates(keep='first', inplace=True)
#
sub_rnd.dropna(subset=['sub_rnd_clean'], inplace=True)

print('sub_merge')

sub_rnd = pd.merge(
        sub_rnd,
        sub_ids[['sub_bvd9', 'bvd9', 'sub_country_2DID_iso', 'sub_world_player']],
        left_on=['sub_bvd9', 'bvd9'], right_on=['sub_bvd9', 'bvd9'],
        how='left',
        suffixes=(False, False)
    )

print('parent_merge')

sub_rnd = pd.merge(
    sub_rnd,
    parent_ids[['bvd9', 'company_name', 'parent_conso', 'is_quoted', 'is_GUO', 'country_2DID_iso', 'world_player',
                'guo_bvd9']],
    left_on=['bvd9', 'parent_conso'], right_on=['bvd9', 'parent_conso'],
    how='left',
    suffixes=(False, False)
).rename(columns={'company_name': 'parent_company_name', 'country_2DID_iso': 'parent_country_2DID_iso',
                  'world_player': 'parent_world_player', 'is_quoted': 'is_parent_quoted', 'is_GUO': 'is_parent_GUO'})

sub_rnd = pd.merge(
    sub_rnd,
    guo_ids[
        ['guo_bvd9', 'guo_name', 'guo_conso', 'is_quoted', 'guo_country_2DID_iso', 'guo_world_player', 'is_top_2000',
         'is_top_100']],
    left_on='guo_bvd9', right_on='guo_bvd9',
    how='left',
    suffixes=(False, False)
).rename(columns={'is_quoted': 'is_guo_quoted'})

# Save output tables

print('Save output file - sub_rnd table ... ')

sub_rnd.to_csv(reg.sub_rnd_path,
               columns=col.sub_rnd,
               float_format='%.10f',
               index=False,
               na_rep='#N/A'
               )




if not reg.guo_rnd_path.exists():

    guo_rnd = rd_mtd.compute_guo_rnd(
        parent_rnd,
        parent_ids,
        guo_ids
    )

    print('Save output file - guo_rnd table ... ')

    guo_rnd.to_csv(reg.guo_rnd_path,
                   columns=col.guo_rnd,
                   float_format='%.10f',
                   index=False,
                   na_rep='#N/A'
                   )

print('Read file - guo_rnd table ... ')

guo_rnd = pd.read_csv(
    reg.guo_rnd_path,
    na_values='#N/A',
    dtype=col.dtype
)