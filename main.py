# Import libraries
import pandas as pd
import method as mtd
import config as cfg
import report as rpt
import input
import datetime
import json
import sys

# <editor-fold desc="#0 - Initialisation">
print('#0 - Initialisation')

# Set  dataframe display options
pd.options.display.max_columns = None
pd.options.display.width=None

# Load config files
(cases, files) = cfg.init()

# Initialize report
report = {}

if cases['CASE_ROOT'].joinpath(r'report.json').exists():
    # Load existing file
    with open(cases['CASE_ROOT'].joinpath(r'report.json'), 'r') as file:
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

rpt.update(report, cases)

# Load keywords for activity screening
with open(cases['BASE'].joinpath(r'keywords.json'), 'r') as file:
    keywords = json.load(file)

# Define data ranges
print('Define data ranges ...')

range_ys = {
    'rnd_ys': ['rnd_y' + str(YY) for YY in range(int(cases['YEAR_FIRST'][-2:]), int(cases['YEAR_LAST'][-2:]) + 1)],
    'oprev_ys': ['op_revenue_y' + str(YY) for YY in
                 range(int(cases['YEAR_FIRST'][-2:]), int(cases['YEAR_LAST'][-2:]) + 1)],
    'LY': str(cases['YEAR_LAST'])[-2:]
}

# Import mapping tables
print('Read country mapping table ...')

country_map = pd.read_csv('https://raw.githubusercontent.com/pysleto/mapping-tables/master/country_table.csv',
                          error_bad_lines=False)

# Initialize final consolidation
sub_rnd = pd.DataFrame()
# </editor-fold>

# <editor-fold desc="#1 - Select parent companies">
print('#1 - Select parent companies')

report['select_parents'] = {}

# Select parent companies
if not files['OUTPUT']['PARENTS']['ID'].exists():
    (report['select_parents'], parent_ids, parent_guo_ids) = mtd.load_parent_ids(cases, files, country_map)
    rpt.update(report, cases)
else:
    print('Read from file ...')
    parent_ids = pd.read_csv(
        files['OUTPUT']['PARENTS']['ID'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
        }
    )

    parent_guo_ids = pd.read_csv(
        files['OUTPUT']['PARENTS']['GUO'],
        na_values='n.a.',
        dtype={
            col: str for col in ['guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id']
        }
    )

parent_id_cols = list(parent_ids.columns)

parent_ids.to_csv(files['OUTPUT']['PARENTS']['BVD9_FULL'],
                  columns=['bvd9'],
                  float_format='%.10f',
                  index=False,
                  na_rep='n.a.'
                  )
# </editor-fold>

# <editor-fold desc="#2 - Load parent company financials">
print('#2 - Load parent company financials')

if not files['OUTPUT']['PARENTS']['FIN'].exists():
    (report['load_parent_financials'], parent_fins) = mtd.load_parent_fins(cases, files, range_ys)

    selected_parent_ids = mtd.select_parent_ids_with_rnd(parent_fins, cases['RND_LIMIT'])

    selected_parent_ids.to_csv(files['OUTPUT']['PARENTS']['BVD9_SHORT'],
                               columns=['bvd9'],
                               float_format='%.10f',
                               index=False,
                               na_rep='n.a.'
                               )

    # select = parent_fins[parent_fins['bvd9'].isin(parent_ids['bvd9'])]
    #
    # report['load_parent_financials']['With financials'] = {
    #     'total_bvd9': parent_fins['bvd9'].nunique(),
    #     'total_rnd_y' + str(cases['YEAR_LAST'])[-2:]: parent_fins['rnd_y' + str(cases['YEAR_LAST'])[-2:]].sum(),
    #     'selected_bvd9': select['bvd9'].nunique(),
    #     'selected_rnd_y' + str(cases['YEAR_LAST'])[-2:]: select['rnd_y' + str(cases['YEAR_LAST'])[-2:]].sum()
    # }

    rpt.update(report, cases)
else:
    print('Read from file ...')
    parent_fins = pd.read_csv(
        files['OUTPUT']['PARENTS']['FIN'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    selected_parent_ids = pd.read_csv(
        files['OUTPUT']['PARENTS']['BVD9_SHORT'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9']
        }
    )

parent_fin_cols = list(parent_fins.columns)
# </editor-fold>

# <editor-fold desc="#3 - Load subsidiary identification and flag for calculation methods">
print('#3 - Load subsidiary identification')

if not files['OUTPUT']['SUBS']['ID'].exists():
    (report['load_subsidiary_identification'], sub_ids) = mtd.load_sub_ids(cases, files, country_map)

    selected_sub_ids = sub_ids[sub_ids['bvd9'].isin(selected_parent_ids['bvd9'])]

    (report['screen_subsidiaries_for_method'], selected_sub_ids) = mtd.screen_sub_ids_for_method(cases, files, selected_sub_ids)

    rpt.update(report, cases)

    # Save lists of subsidiary bvd9 ids
    sub_ids.to_csv(files['OUTPUT']['SUBS']['BVD9_FULL'],
                   columns=['sub_bvd9'],
                   float_format='%.10f',
                   index=False,
                   na_rep='n.a.'
                   )

    selected_sub_ids.to_csv(files['OUTPUT']['SUBS']['BVD9_SHORT'],
                            columns=['sub_bvd9'],
                            float_format='%.10f',
                            index=False,
                            na_rep='n.a.'
                            )

    # Update retrieved subsidiary count in parent_fins
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

        parent_ids.to_csv(files['OUTPUT']['PARENTS']['ID'],
                          columns=parent_id_cols,
                          float_format='%.10f',
                          index=False,
                          na_rep='n.a.'
                          )
else:
    print('Read from file ...')
    selected_sub_ids = pd.read_csv(
        files['OUTPUT']['SUBS']['ID'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_NACE_4Dcode']
        }
    )

selected_sub_id_cols = list(selected_sub_ids.columns)
# </editor-fold>

# <editor-fold desc="#4 - Load subsidiary financials and screen keywords in activity">
print('#4 - Load subsidiary financials')

if not files['OUTPUT']['SUBS']['FIN'].exists():
    (report['load_subsidiary_financials'], sub_fins) = mtd.load_sub_fins(cases, files, range_ys)
    (report['screen_subsidiary_activities'], sub_fins) = mtd.screen_sub_fins_for_keywords(cases, files, range_ys,
                                                                                          keywords,
                                                                                          sub_fins)

    rpt.update(report, cases)
else:
    print('Read from file ...')
    sub_fins = pd.read_csv(
        files['OUTPUT']['SUBS']['FIN'],
        na_values='n.a.',
        dtype={
            col: str for col in ['sub_bvd9', 'sub_bvd_id']
        }
    )

sub_fin_cols = list(sub_fins.columns)
# </editor-fold>

# <editor-fold desc="#5 - Calculating group and subsidiary level exposure">
print('#5 - Calculating group and subsidiary level exposure')

# Loading exposure at subsidiary and parent company level
if not (files['OUTPUT']['PARENTS']['EXPO'].exists() & files['OUTPUT']['SUBS']['EXPO'].exists()):
    (report['keyword_screen_by_method'], report['compute_exposure'], parent_exposure, sub_exposure) = \
        mtd.compute_exposure(
            cases,
            files,
            range_ys,
            selected_sub_ids,
            sub_fins[sub_fins['sub_bvd9'].isin(selected_sub_ids['sub_bvd9'])]
        )

    rpt.update(report, cases)
else:
    print('Read from files ...')

    parent_exposure = pd.read_csv(
        files['OUTPUT']['PARENTS']['EXPO'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    sub_exposure = pd.read_csv(
        files['OUTPUT']['SUBS']['EXPO'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )
# </editor-fold>

# <editor-fold desc="#6 - Calculating group and subsidiary level rnd">
print('#6 - Calculating group and subsidiary level rnd')

if not files['OUTPUT']['PARENTS']['RND'].exists():
    report['compute_rnd'] = {}

    (report['compute_rnd']['at_parent_level'], parent_rnd) = mtd.compute_parent_rnd(
        cases,
        files,
        range_ys,
        parent_exposure,
        parent_fins[parent_fins['bvd9'].isin(selected_parent_ids['bvd9'])]
    )

    rpt.update(report, cases)
else:
    print('Read from file ...')

    parent_rnd = pd.read_csv(
        files['OUTPUT']['PARENTS']['RND'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9']
        }
    )

if not files['OUTPUT']['SUBS']['RND'].exists():
    (report['compute_rnd']['at_subsidiary_level'], sub_rnd) = mtd.compute_sub_rnd(cases, files, range_ys, sub_exposure,
                                                                                  parent_rnd)

    rpt.update(report, cases)
else:
    print('Read from file ...')

    sub_rnd = pd.read_csv(
        files['OUTPUT']['SUBS']['RND'],
        na_values='n.a.',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )
# </editor-fold>

# <editor-fold desc="#7 - Final reporting and consolidation">
print('#7 - Final reporting and consolidation')

# Import soeur_rnd for benchmark
print('Read soeur_rnd benchmark table ...')

soeur_rnd_grouped = pd.read_csv(
    'https://raw.githubusercontent.com/pysleto/mapping-tables/master/SOEUR_RnD_20191206%20-%20grouped.csv',
    error_bad_lines=False)

# if not files['SOEUR_RND']['ROOT'].joinpath(files['SOEUR_RND']['VERSION'] + '- full.csv').exists():
#     soeur_rnd = rpt.load_soeur_rnd(cases, files, country_map)
# else:
#     print('Read from file ...')
#     soeur_rnd = pd.read_csv(
#         files['SOEUR_RND']['ROOT'].joinpath(files['SOEUR_RND']['VERSION'] + ' - full.csv'),
#         na_values='n.a.',
#         dtype={
#             col: str for col in ['jrc_id']
#         }
#     )
#
# soeur_rnd_grouped_cols = ['year', 'sub_country_3DID_iso', 'sub_world_player', 'technology', 'action', 'priority']
#
# soeur_rnd_grouped = soeur_rnd.groupby(soeur_rnd_grouped_cols).sum()
#
# soeur_rnd_grouped.reset_index(inplace=True)
#
# print('Save soeur_rnd_grouped output files ...')
#
# soeur_rnd_grouped.to_csv(files['SOEUR_RND']['ROOT'].joinpath(files['SOEUR_RND']['VERSION'] + ' - grouped.csv'),
#                          columns=soeur_rnd_grouped_cols + ['sub_rnd_clean'],
#                          float_format='%.10f',
#                          index=False,
#                          na_rep='n.a.'
#                          )

sub_rnd_grouped_w_bvd9 = rpt.group_sub_rnd_by_approach(
    cases,
    files,
    keywords,
    soeur_rnd_grouped,
    sub_rnd,
    parent_ids,
    parent_guo_ids,
    selected_sub_ids,
    sub_fins[sub_fins['sub_bvd9'].isin(selected_sub_ids['sub_bvd9'])],
    country_map
)

print(sub_rnd_grouped_w_bvd9.head())

rpt.get_group_rnd_distribution(
    cases,
    files,
    keywords,
    parent_ids,
    parent_rnd,
    sub_rnd_grouped_w_bvd9
)

with open(cases['CASE_ROOT'].joinpath(r'report.json'), 'r') as file:
    report = json.load(file)

rpt.pprint(report, cases)
# </editor-fold>


sys.exit('My break')