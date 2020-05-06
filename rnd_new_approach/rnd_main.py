# Import libraries

import pandas as pd
import datetime
import json

from rnd_new_approach import rnd_methods as mtd
import config as cfg

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

if cases['case_root'].joinpath(r'report.json').exists():
    # Load existing file
    with open(cases['case_root'].joinpath(r'report.json'), 'r') as file:
        report = json.load(file)

    # Update time stamp
    report['initialisation']['Datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
else:
    cases_to_str = {}

    for key in cases.keys():
        cases_to_str[key] = str(cases[key])

    report['initialisation'] = {
        'Datetime': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'Use case': cases_to_str
    }

mtd.update_report(report, cases)

# Load keywords for activity screening
with open(cases['base'].joinpath(r'keywords.json'), 'r') as file:
    keywords = json.load(file)

# Define data ranges
print('Define data ranges ...')

range_ys = {
    'rnd_ys': ['rnd_y' + str(YY) for YY in range(int(cases['year_first'][-2:]), int(cases['year_last'][-2:]) + 1)],
    'oprev_ys': ['op_revenue_y' + str(YY) for YY in
                 range(int(cases['year_first'][-2:]), int(cases['year_last'][-2:]) + 1)],
    'LY': str(cases['year_last'])[-2:]
}

# Import mapping tables
print('Read country mapping table ...')

country_map = pd.read_csv('https://raw.githubusercontent.com/pysleto/rnd-private/master/ref_tables/country_table.csv',
                          error_bad_lines=False)

# Initialize final consolidation
sub_rnd = pd.DataFrame()
# </editor-fold>

# <editor-fold desc="#1 - Select parent companies">
print('#1 - Select parent companies')

report['select_parents'] = {}

# Select parent companies
if not files['rnd_outputs']['parents']['id'].exists():
    (report['select_parents'], parent_ids, parent_guo_ids) = mtd.load_parent_ids(cases, files, country_map)
    mtd.update_report(report, cases)
else:
    print('Read from file ...')
    parent_ids = pd.read_csv(
        files['rnd_outputs']['parents']['id'],
        na_values='#N/A',
        dtype={
            col: str for col in ['guo_bvd9', 'bvd9', 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
        }
    )

    parent_guo_ids = pd.read_csv(
        files['rnd_outputs']['parents']['guo'],
        na_values='#N/A',
        dtype={
            col: str for col in ['guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id']
        }
    )

parent_id_cols = list(parent_ids.columns)

pd.Series(parent_ids.bvd9.unique()).to_csv(files['rnd_outputs']['parents']['bvd9_full'],
                                           index=False,
                                           header=False,
                                           na_rep='#N/A'
                                           )
# </editor-fold>

# <editor-fold desc="#2 - Load parent company financials">
print('#2 - Load parent company financials')

if not files['rnd_outputs']['parents']['fin'].exists():
    (report['load_parent_financials'], parent_fins) = mtd.load_parent_fins(cases, files, range_ys)

    # TODO: Check that selected parent ids based on rnd_limit is representative of total rnd in each world region
    selected_parent_ids = mtd.select_parent_ids_with_rnd(parent_fins, cases['rnd_limit'])

    selected_parent_bvd9_ids = pd.Series(selected_parent_ids.bvd9.unique())

    selected_parent_bvd9_ids.to_csv(files['rnd_outputs']['parents']['bvd9_short'],
                                    float_format='%.10f',
                                    index=False,
                                    header=False,
                                    na_rep='#N/A'
                                    )

    # select = parent_fins[parent_fins['bvd9'].isin(parent_ids['bvd9'])]
    #
    # report['load_parent_financials']['With financials'] = {
    #     'total_bvd9': parent_fins['bvd9'].nunique(),
    #     'total_rnd_y' + str(cases['year_last'])[-2:]: parent_fins['rnd_y' + str(cases['year_last'])[-2:]].sum(),
    #     'selected_bvd9': select['bvd9'].nunique(),
    #     'selected_rnd_y' + str(cases['year_last'])[-2:]: select['rnd_y' + str(cases['year_last'])[-2:]].sum()
    # }

    mtd.update_report(report, cases)
else:
    print('Read from file ...')
    parent_fins = pd.read_csv(
        files['rnd_outputs']['parents']['fin'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    selected_parent_bvd9_ids = pd.read_csv(
        files['rnd_outputs']['parents']['bvd9_short'],
        na_values='#N/A',
        header=None,
        dtype=str
    )[0]

parent_fin_cols = list(parent_fins.columns)
# </editor-fold>

# <editor-fold desc="#3 - Load subsidiary identification and flag for calculation methods">
print('#3 - Load subsidiary identification')

if not files['rnd_outputs']['subs']['id'].exists():
    (report['load_subsidiary_identification'], sub_ids) = mtd.load_sub_ids(cases, files, country_map)

    selected_sub_ids = sub_ids[sub_ids.bvd9.isin(selected_parent_bvd9_ids)]

    (report['screen_subsidiaries_for_method'], sub_ids) = mtd.screen_sub_ids_for_method(cases, files, parent_ids,
                                                                                        sub_ids)

    mtd.update_report(report, cases)

    # Save lists of subsidiary bvd9 ids
    sub_bvd9_ids = pd.Series(sub_ids.sub_bvd9.unique())

    sub_bvd9_ids.to_csv(files['rnd_outputs']['subs']['bvd9_full'],
                        index=False,
                        header=False,
                        na_rep='#N/A'
                        )

    selected_sub_bvd9_ids = pd.Series(selected_sub_ids.sub_bvd9.unique())

    selected_sub_bvd9_ids.to_csv(files['rnd_outputs']['subs']['bvd9_short'],
                                 index=False,
                                 header=False,
                                 na_rep='#N/A'
                                 )

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
    # TODO: Implement check and update of a MNC reference table
    # Flag parent_ids that are keep_sub to consolidate a unique list of MNCs
    if 'is_MNC' not in parent_id_cols:
        parent_id_cols.insert(parent_id_cols.index('guo_bvd9') + 1, 'is_MNC')

        parent_ids['is_MNC'] = True

        parent_ids.loc[parent_ids['bvd9'].isin(sub_ids['sub_bvd9']), 'is_MNC'] = False

    # Update parent_ids output file
    parent_ids.to_csv(files['rnd_outputs']['parents']['id'],
                      columns=parent_id_cols,
                      float_format='%.10f',
                      index=False,
                      na_rep='#N/A'
                      )
else:
    print('Read from file ...')
    sub_ids = pd.read_csv(
        files['rnd_outputs']['subs']['id'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_NACE_4Dcode']
        }
    )

    selected_sub_bvd9_ids = pd.read_csv(files['rnd_outputs']['subs']['bvd9_short'],
                                        na_values='#N/A',
                                        header=None,
                                        dtype=str
                                        )[0]

    selected_sub_ids = sub_ids[sub_ids.sub_bvd9.isin(selected_sub_bvd9_ids)]

selected_sub_id_cols = list(selected_sub_ids.columns)
# </editor-fold>

# <editor-fold desc="#4 - Load subsidiary financials and screen keywords in activity">
print('#4 - Load subsidiary financials')

if not files['rnd_outputs']['subs']['fin'].exists():
    (report['load_subsidiary_financials'], sub_fins) = mtd.load_sub_fins(cases, files, range_ys)
    (report['screen_subsidiary_activities'], sub_fins) = mtd.screen_sub_fins_for_keywords(cases, files, range_ys,
                                                                                          keywords,
                                                                                          sub_fins)

    mtd.update_report(report, cases)
else:
    print('Read from file ...')
    sub_fins = pd.read_csv(
        files['rnd_outputs']['subs']['fin'],
        na_values='#N/A',
        dtype={
            col: str for col in ['sub_bvd9', 'sub_bvd_id']
        }
    )

sub_fin_cols = list(sub_fins.columns)
# </editor-fold>

# <editor-fold desc="#5 - Calculating group and subsidiary level exposure">
print('#5 - Calculating group and subsidiary level exposure')

# TODO: integrate parents that are MNC but do not have subsidiaries (therefore are not managed by keep_sub) in exposure and rnd calculations
# Loading exposure at subsidiary and parent company level
if not (files['rnd_outputs']['parents']['expo'].exists() & files['rnd_outputs']['subs']['expo'].exists()):
    (report['keyword_screen_by_method'], report['compute_exposure'], parent_exposure, sub_exposure) = \
        mtd.compute_exposure(
            cases,
            files,
            range_ys,
            sub_ids,
            sub_fins
        )

    mtd.update_report(report, cases)
else:
    print('Read from files ...')

    parent_exposure = pd.read_csv(
        files['rnd_outputs']['parents']['expo'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    sub_exposure = pd.read_csv(
        files['rnd_outputs']['subs']['expo'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )
# </editor-fold>

# <editor-fold desc="#6 - Calculating group and subsidiary level rnd">
print('#6 - Calculating group and subsidiary level rnd')

if not files['rnd_outputs']['parents']['rnd'].exists():
    report['compute_rnd'] = {}

    (report['compute_rnd']['at_parent_level'], parent_rnd) = mtd.compute_parent_rnd(
        cases,
        files,
        range_ys,
        parent_exposure,
        parent_fins
    )

    mtd.update_report(report, cases)
else:
    print('Read from file ...')

    parent_rnd = pd.read_csv(
        files['rnd_outputs']['parents']['rnd'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

if not files['rnd_outputs']['subs']['rnd'].exists():
    (report['compute_rnd']['at_subsidiary_level'], sub_rnd) = mtd.compute_sub_rnd(cases, files, range_ys, sub_exposure,
                                                                                  parent_rnd)

    mtd.update_report(report, cases)
else:
    print('Read from file ...')

    sub_rnd = pd.read_csv(
        files['rnd_outputs']['subs']['rnd'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )
# </editor-fold>