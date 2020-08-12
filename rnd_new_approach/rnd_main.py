# Import libraries
from config import registry as reg
from config import col_ids as col

import pandas as pd

from rnd_new_approach import rnd_methods as rd_mtd
from benchmark import by_methods as by_mtd

# TODO: de couple ORBIS parent and subs consolidation from rnd
# TODO: import previous name to integrate name change in fuzzy match
# TODO: GUI to prompt user for use_case and place instead of cfg.init hard coding
# TODO: Abstract main.py with object class and functions for patterns
# TODO: Progress bars for reading input files by chunks
# TODO: Implement .index over dataframes

# <editor-fold desc="#0 - Initialisation">
print('#0 - Initialisation')

# # Initialize report
# report = {}

# if reg.case_path.joinpath(r'report.json').exists():
#     # Load existing file
#     with open(reg.case_path.joinpath(r'report.json'), 'r') as file:
#         report = json.load(file)
#
#     # Update time stamp
#     report['initialisation']['Datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# else:
#     reg_to_str = {}

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

# Initialize final consolidation
sub_rnd = pd.DataFrame()
# </editor-fold>

# <editor-fold desc="#1 - Select parent companies">
print('#1 - Select parent companies')

# report['select_parents'] = {}

# Select parent companies
if not reg.parent_id_path.exists():
    # (report['select_parents'], parent_ids, guo_ids) = rd_mtd.load_parent_ids()
    (parent_ids, guo_ids) = rd_mtd.load_parent_ids()
    # rd_mtd.update_report(report)

    print('Save output files ...')

    parent_ids = by_mtd.select_by_account(parent_ids, 'parent')

    parent_ids.to_csv(reg.parent_id_path,
                      columns=col.parent_ids + ['country_3DID_iso', 'world_player'],
                      float_format='%.10f',
                      index=False,
                      na_rep='#N/A'
                      )

    print('Save output files ...')

    guo_ids = by_mtd.select_by_account(guo_ids, 'guo')

    guo_ids.to_csv(reg.parent_guo_path,
                   columns=col.guo_ids + ['guo_country_3DID_iso', 'guo_world_player'],
                   float_format='%.10f',
                   index=False,
                   na_rep='#N/A'
                   )
else:
    print('Read from file ...')
    parent_ids = pd.read_csv(
        reg.parent_id_path,
        na_values='#N/A',
        dtype=col.dtype
    )

    guo_ids = pd.read_csv(
        reg.parent_guo_path,
        na_values='#N/A',
        dtype=col.dtype
    )

pd.Series(parent_ids.bvd9.unique()).to_csv(reg.parent_bvd9_full_path,
                                           index=False,
                                           header=False,
                                           na_rep='#N/A'
                                           )
# </editor-fold>

# <editor-fold desc="#2 - Load parent company financials">
print('#2 - Load parent company financials')

if not reg.parent_fin_path.exists():
    parent_fins = rd_mtd.load_parent_fins()

    parent_fins.sort_values(by=['oprev_sum', 'rnd_sum'], ascending=False, na_position='last', inplace=True)

    parent_fins = by_mtd.select_by_account(parent_fins, 'parent')

    top_rnd_bvd9 = parent_fins.nlargest(2000, ['rnd_sum'])
    top_rnd_bvd9['level'] = 'parent'
    top_rnd_bvd9 = top_rnd_bvd9[['bvd9', 'level']]

    print('Save output file ...')

    # Save it as csv
    top_rnd_bvd9.to_csv(reg.case_path.joinpath('bvd9 - select top 2000 total rnd.csv'),
                        float_format='%.10f',
                        index=False,
                        na_rep='#N/A'
                        )

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
else:
    print('Read from file ...')
    parent_fins = pd.read_csv(
        reg.parent_fin_path,
        na_values='#N/A',
        dtype=col.dtype
    )

    top_rnd_bvd9 = pd.read_csv(
        reg.case_path.joinpath('bvd9 - select top 2000 total rnd.csv'),
        na_values='#N/A',
        dtype=str
    )

    # selected_parent_bvd9_ids = pd.read_csv(
    #     reg.parent_bvd9_short_path,
    #     na_values='#N/A',
    #     header=None,
    #     dtype=str
    # )[0]
# </editor-fold>

# <editor-fold desc="#3 - Load subsidiary identification and flag for calculation methods">
print('#3 - Load subsidiary identification')

if not reg.sub_id_path.exists():
    # (report['load_subsidiary_identification'], sub_ids) = rd_mtd.load_sub_ids()
    sub_ids = rd_mtd.load_sub_ids()

    # selected_sub_ids = sub_ids[sub_ids.bvd9.isin(selected_parent_bvd9_ids)]

    sub_ids = rd_mtd.screen_sub_ids_for_method(parent_ids, sub_ids)
    # (report['screen_subsidiaries_for_method'], sub_ids) = rd_mtd.screen_sub_ids_for_method(parent_ids, sub_ids)
    #
    # rd_mtd.update_report(report)

    # Identifying the subsidiaries that are embedded in parent top 2000 R&D investors
    sub_in_top_rnd_bvd9 = sub_ids[sub_ids['bvd9'].isin(top_rnd_bvd9['bvd9'])]
    top_rnd_bvd9_sub = pd.DataFrame()
    top_rnd_bvd9_sub['bvd9'] = sub_in_top_rnd_bvd9['sub_bvd9']
    top_rnd_bvd9_sub['level'] = 'sub'
    top_rnd_bvd9 = top_rnd_bvd9.append(top_rnd_bvd9_sub)

    top_rnd_bvd9.to_csv(reg.case_path.joinpath('bvd9 - select top 2000 total rnd.csv'),
                        float_format='%.10f',
                        index=False,
                        na_rep='#N/A'
                        )

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

    print('Save output file ...')

    # Save lists of subsidiary bvd9 ids
    sub_bvd9_ids = pd.Series(sub_ids.sub_bvd9.unique())

    sub_bvd9_ids.to_csv(reg.sub_bvd9_full_path,
                        index=False,
                        header=False,
                        na_rep='#N/A'
                        )

    # TODO: Check for subsidiaries that have the same name but several corresponding bvd9 ids
    # Save it as csv
    sub_ids.to_csv(reg.sub_id_path,
                   columns=col.sub_ids,
                   float_format='%.10f',
                   index=False,
                   na_rep='#N/A'
                   )
else:
    print('Read from file ...')
    sub_ids = pd.read_csv(
        reg.sub_id_path,
        na_values='#N/A',
        dtype=col.dtype
    )

    top_rnd_bvd9 = pd.read_csv(
        reg.case_path.joinpath('bvd9 - select top 2000 total rnd.csv'),
        na_values='#N/A',
        dtype=str
    )

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

# <editor-fold desc="#4 - Load subsidiary financials and screen keywords in activity">
print('#4 - Load subsidiary financials')

if not reg.sub_fin_path.exists():
    sub_fins = rd_mtd.load_sub_fins()

    sub_fins.sort_values(by=['oprev_sum', 'rnd_sum'], ascending=False, na_position='last', inplace=True)

    sub_fins = by_mtd.select_by_account(sub_fins, 'sub')

    sub_fins = rd_mtd.screen_sub_fins_for_keywords(sub_fins)
    # (report['load_subsidiary_financials'], sub_fins) = rd_mtd.load_sub_fins()
    # (report['screen_subsidiary_activities'], sub_fins) = rd_mtd.screen_sub_fins_for_keywords(sub_fins)
    #
    # rd_mtd.update_report(report)

    print('Save output file ...')

    # Save it as csv
    sub_fins.to_csv(reg.sub_fin_path,
                    columns=col.sub_fins,
                    float_format='%.10f',
                    index=False,
                    na_rep='#N/A'
                    )

else:
    print('Read from file ...')
    sub_fins = pd.read_csv(
        reg.sub_fin_path,
        na_values='#N/A',
        dtype=col.dtype
    )
# </editor-fold>

# <editor-fold desc="#5 - Calculating group and subsidiary level exposure">
print('#5 - Calculating group and subsidiary level exposure')

# TODO: integrate parents that do not have subsidiaries (therefore are not managed by keep_sub) in exposure and rnd calculations
# Loading exposure at subsidiary and parent company level
if not (reg.parent_expo_path.exists() & reg.sub_expo_path.exists()):
    (parent_exposure, sub_exposure) = rd_mtd.compute_exposure(
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

    print('Save output file ...')

    # Save output tables
    parent_exposure.to_csv(reg.parent_expo_path,
                           columns=col.parent_exp,
                           float_format='%.10f',
                           index=False,
                           na_rep='#N/A'
                           )

    sub_exposure.to_csv(reg.sub_expo_path,
                        columns=col.sub_exp,
                        float_format='%.10f',
                        index=False,
                        na_rep='#N/A'
                        )

else:
    print('Read from files ...')

    parent_exposure = pd.read_csv(
        reg.parent_expo_path,
        na_values='#N/A',
        dtype=col.dtype
    )

    sub_exposure = pd.read_csv(
        reg.sub_expo_path,
        na_values='#N/A',
        dtype=col.dtype
    )
# </editor-fold>

# <editor-fold desc="#6 - Calculating group and subsidiary level rnd">
print('#6 - Calculating group and subsidiary level rnd')

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

    parent_rnd.to_csv(reg.parent_rnd_path,
                      columns=col.parent_rnd,
                      float_format='%.10f',
                      index=False,
                      na_rep='#N/A'
                      )
else:
    print('Read from file ...')

    parent_rnd = pd.read_csv(
        reg.parent_rnd_path,
        na_values='#N/A',
        dtype=col.dtype
    )


sub_rnd = rd_mtd.compute_sub_rnd(sub_exposure, parent_rnd)

# (report['compute_rnd']['at_subsidiary_level'], sub_rnd) = rd_mtd.compute_sub_rnd(sub_exposure,
#                                                                               parent_rnd)
#
# rd_mtd.update_report(report)

sub_rnd['is_sub_top'] = sub_rnd['sub_bvd9'].isin(top_rnd_bvd9.loc[top_rnd_bvd9['level'] == 'sub', 'bvd9'])
sub_rnd['is_parent_top'] = sub_rnd['bvd9'].isin(top_rnd_bvd9.loc[top_rnd_bvd9['level'] == 'parent', 'bvd9'])
sub_rnd['is_in_top'] = sub_rnd['is_sub_top'] | sub_rnd['is_parent_top']

# Save output tables
sub_rnd.dropna(subset=['sub_rnd_clean']).to_csv(reg.sub_rnd_path,
                                                columns=col.sub_rnd,
                                                float_format='%.10f',
                                                index=False,
                                                na_rep='#N/A'
                                                )
# </editor-fold>

