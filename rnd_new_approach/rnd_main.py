# Import libraries

import pandas as pd
import datetime
import json

from rnd_new_approach import rnd_methods as mtd
import init_config as cfg

# TODO: de couple ORBIS parent and subs consolidation from rnd
# TODO: import previous name to integrate name change in fuzzy match
# TODO: GUI to prompt user for use_case and place instead of cfg.init hard coding
# TODO: Abstract main.py with object class and functions for patterns
# TODO: Progress bars for reading input files by chunks
# TODO: Implement .index over dataframes

# <editor-fold desc="#0 - Initialisation">
print('#0 - Initialisation')

# Set  DataFrame display options
pd.options.display.max_columns = None
pd.options.display.width = None

# Load config files
(cases, files) = cfg.init()

# Initialize report
report = {}

# Load config files
reg = cfg.load_my_registry()

if reg['case_root'].joinpath(r'report.json').exists():
    # Load existing file
    with open(reg['case_root'].joinpath(r'report.json'), 'r') as file:
        report = json.load(file)

    # Update time stamp
    report['initialisation']['Datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
else:
    reg_to_str = {}

    for key in reg.keys():
        reg_to_str[key] = str(reg[key])

    report['initialisation'] = {
        'Datetime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'Use case': reg_to_str
    }

mtd.update_report(report)

# Initialize final consolidation
sub_rnd = pd.DataFrame()
# </editor-fold>

# <editor-fold desc="#1 - Select parent companies">
print('#1 - Select parent companies')

# report['select_parents'] = {}

# Select parent companies
if not reg['parent']['id'].exists():
    (report['select_parents'], parent_ids, parent_guo_ids) = mtd.load_parent_ids()
    mtd.update_report(report)
else:
    print('Read from file ...')
    parent_ids = pd.read_csv(
        reg['parent']['id'],
        na_values='#N/A',
        dtype={
            col: str for col in ['guo_bvd9', 'bvd9', 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
        }
    )

    parent_guo_ids = pd.read_csv(
        reg['parent']['guo'],
        na_values='#N/A',
        dtype={
            col: str for col in ['guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id']
        }
    )

parent_id_cols = list(parent_ids.columns)

pd.Series(parent_ids.bvd9.unique()).to_csv(reg['parent']['bvd9_full'],
                                           index=False,
                                           header=False,
                                           na_rep='#N/A'
                                           )
# </editor-fold>

# <editor-fold desc="#2 - Load parent company financials">
print('#2 - Load parent company financials')

if not reg['parent']['fin'].exists():
    (report['load_parent_financials'], parent_fins) = mtd.load_parent_fins()

    # TODO: Check that selected parent ids based on rnd_limit is representative of total rnd in each world region
    # selected_parent_ids = mtd.select_parent_ids_with_rnd(parent_fins)
    #
    # selected_parent_bvd9_ids = pd.Series(selected_parent_ids.bvd9.unique())
    #
    # selected_parent_bvd9_ids.to_csv(reg['parent']['bvd9_short'],
    #                                 float_format='%.10f',
    #                                 index=False,
    #                                 header=False,
    #                                 na_rep='#N/A'
    #                                 )

    # select = parent_fins[parent_fins['bvd9'].isin(parent_ids['bvd9'])]
    #
    # report['load_parent_financials']['With financials'] = {
    #     'total_bvd9': parent_fins['bvd9'].nunique(),
    #     'total_rnd_y' + str(reg['year_last'])[-2:]: parent_fins['rnd_y' + str(reg['year_last'])[-2:]].sum(),
    #     'selected_bvd9': select['bvd9'].nunique(),
    #     'selected_rnd_y' + str(reg['year_last'])[-2:]: select['rnd_y' + str(reg['year_last'])[-2:]].sum()
    # }

    mtd.update_report(report)
else:
    print('Read from file ...')
    parent_fins = pd.read_csv(
        reg['parent']['fin'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    # selected_parent_bvd9_ids = pd.read_csv(
    #     reg['parent']['bvd9_short'],
    #     na_values='#N/A',
    #     header=None,
    #     dtype=str
    # )[0]

parent_fin_cols = list(parent_fins.columns)
# </editor-fold>

# <editor-fold desc="#3 - Load subsidiary identification and flag for calculation methods">
print('#3 - Load subsidiary identification')

if not reg['sub']['id'].exists():
    (report['load_subsidiary_identification'], sub_ids) = mtd.load_sub_ids()

    # selected_sub_ids = sub_ids[sub_ids.bvd9.isin(selected_parent_bvd9_ids)]

    (report['screen_subsidiaries_for_method'], sub_ids) = mtd.screen_sub_ids_for_method(parent_ids, sub_ids)

    mtd.update_report(report)

    # Save lists of subsidiary bvd9 ids
    sub_bvd9_ids = pd.Series(sub_ids.sub_bvd9.unique())

    sub_bvd9_ids.to_csv(reg['sub']['bvd9_full'],
                        index=False,
                        header=False,
                        na_rep='#N/A'
                        )

    # selected_sub_bvd9_ids = pd.Series(selected_sub_ids.sub_bvd9.unique())
    #
    # selected_sub_bvd9_ids.to_csv(reg['sub']['bvd9_short'],
    #                              index=False,
    #                              header=False,
    #                              na_rep='#N/A'
    #                              )

    # Update retrieved subsidiary count in parent_ids
    if 'subs_n_collected' not in parent_id_cols:
        parent_id_cols.insert(parent_id_cols.index('subs_n') + 1, 'subs_n_collected')

        parent_ids = pd.merge(
            parent_ids,
            sub_ids[['bvd9', 'sub_bvd9']].groupby(['bvd9']).count().rename(
                columns={'sub_bvd9': 'subs_n_collected'}
            ).reset_index(),
            left_on='bvd9', right_on='bvd9',
            how='left',
            suffixes=(False, False)
        )
else:
    print('Read from file ...')
    sub_ids = pd.read_csv(
        reg['sub']['id'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_NACE_4Dcode']
        }
    )

#     selected_sub_bvd9_ids = pd.read_csv(reg['sub']['bvd9_short'],
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

if not reg['sub']['fin'].exists():
    (report['load_subsidiary_financials'], sub_fins) = mtd.load_sub_fins()
    (report['screen_subsidiary_activities'], sub_fins) = mtd.screen_sub_fins_for_keywords(sub_fins)

    mtd.update_report(report)
else:
    print('Read from file ...')
    sub_fins = pd.read_csv(
        reg['sub']['fin'],
        na_values='#N/A',
        dtype={
            col: str for col in ['sub_bvd9', 'sub_bvd_id']
        }
    )

sub_fin_cols = list(sub_fins.columns)
# </editor-fold>

# <editor-fold desc="#5 - Calculating group and subsidiary level exposure">
print('#5 - Calculating group and subsidiary level exposure')

# TODO: integrate parents that do not have subsidiaries (therefore are not managed by keep_sub) in exposure and rnd calculations
# Loading exposure at subsidiary and parent company level
if not (reg['parent']['expo'].exists() & reg['sub']['expo'].exists()):
    (report['keyword_screen_by_method'], report['compute_exposure'], parent_exposure, sub_exposure) = \
        mtd.compute_exposure(sub_ids,
                             sub_fins
                             )

    mtd.update_report(report)
else:
    print('Read from files ...')

    parent_exposure = pd.read_csv(
        reg['parent']['expo'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    sub_exposure = pd.read_csv(
        reg['sub']['expo'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )
# </editor-fold>

# <editor-fold desc="#6 - Calculating group and subsidiary level rnd">
print('#6 - Calculating group and subsidiary level rnd')

if not reg['parent']['rnd'].exists():
    report['compute_rnd'] = {}

    (report['compute_rnd']['at_parent_level'], parent_rnd) = mtd.compute_parent_rnd(
        parent_exposure,
        parent_fins
    )

    mtd.update_report(report)
else:
    print('Read from file ...')

    parent_rnd = pd.read_csv(
        reg['parent']['rnd'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

if not reg['sub']['rnd'].exists():
    (report['compute_rnd']['at_subsidiary_level'], sub_rnd) = mtd.compute_sub_rnd(sub_exposure,
                                                                                  parent_rnd)

    mtd.update_report(report)
else:
    print('Read from file ...')

    sub_rnd = pd.read_csv(
        reg['sub']['rnd'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )
# </editor-fold>