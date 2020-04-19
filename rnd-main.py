# Import libraries
import pandas as pd
import rnd_new_approach.method as mtd
import rnd_new_approach.config as cfg
import rnd_new_approach.report as rpt
import datetime
import json

# TODO: GUI to prompt user for use_case and place instead of cfg.init hard coding
# TODO: Abstract main.py with object class and functions for patterns
# TODO: Progress bars for reading input files by chunks
# TODO: Implement .index over dataframes

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
        na_values='#N/A',
        dtype={
            col: str for col in ['guo_bvd9', 'bvd9', 'bvd_id', 'legal_entity_id', 'NACE_4Dcode']
        }
    )

    parent_guo_ids = pd.read_csv(
        files['OUTPUT']['PARENTS']['GUO'],
        na_values='#N/A',
        dtype={
            col: str for col in ['guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id']
        }
    )

parent_id_cols = list(parent_ids.columns)

pd.Series(parent_ids.bvd9.unique()).to_csv(files['OUTPUT']['PARENTS']['BVD9_FULL'],
                                           index=False,
                                           header=False,
                                           na_rep='#N/A'
                                           )
# </editor-fold>

# <editor-fold desc="#2 - Load parent company financials">
print('#2 - Load parent company financials')

if not files['OUTPUT']['PARENTS']['FIN'].exists():
    (report['load_parent_financials'], parent_fins) = mtd.load_parent_fins(cases, files, range_ys)

    # TODO: Check that selected parent ids based on rnd_limit is representative of total rnd in each world region
    selected_parent_ids = mtd.select_parent_ids_with_rnd(parent_fins, cases['RND_LIMIT'])

    selected_parent_bvd9_ids = pd.Series(selected_parent_ids.bvd9.unique())

    selected_parent_bvd9_ids.to_csv(files['OUTPUT']['PARENTS']['BVD9_SHORT'],
                                    float_format='%.10f',
                                    index=False,
                                    header=False,
                                    na_rep='#N/A'
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
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    selected_parent_bvd9_ids = pd.read_csv(
        files['OUTPUT']['PARENTS']['BVD9_SHORT'],
        na_values='#N/A',
        header=None,
        dtype=str
    )[0]

parent_fin_cols = list(parent_fins.columns)
# </editor-fold>

# <editor-fold desc="#3 - Load subsidiary identification and flag for calculation methods">
print('#3 - Load subsidiary identification')

if not files['OUTPUT']['SUBS']['ID'].exists():
    (report['load_subsidiary_identification'], sub_ids) = mtd.load_sub_ids(cases, files, country_map)

    selected_sub_ids = sub_ids[sub_ids.bvd9.isin(selected_parent_bvd9_ids)]

    (report['screen_subsidiaries_for_method'], sub_ids) = mtd.screen_sub_ids_for_method(cases, files, sub_ids)

    rpt.update(report, cases)

    # Save lists of subsidiary bvd9 ids
    sub_bvd9_ids = pd.Series(sub_ids.bvd9.unique())

    sub_bvd9_ids.to_csv(files['OUTPUT']['SUBS']['BVD9_FULL'],
                        index=False,
                        header=False,
                        na_rep='#N/A'
                        )

    selected_sub_bvd9_ids = pd.Series(selected_sub_ids.sub_bvd9.unique())

    selected_sub_bvd9_ids.to_csv(files['OUTPUT']['SUBS']['BVD9_SHORT'],
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

        parent_ids['is_MNC'] = parent_ids.bvd9.isin(
            sub_ids.loc[sub_ids['keep_subs'] == True, 'bvd9'].drop_duplicates())

    # Update parent_ids output file
    parent_ids.to_csv(files['OUTPUT']['PARENTS']['ID'],
                      columns=parent_id_cols,
                      float_format='%.10f',
                      index=False,
                      na_rep='#N/A'
                      )
else:
    print('Read from file ...')
    sub_ids = pd.read_csv(
        files['OUTPUT']['SUBS']['ID'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9', 'bvd_id', 'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_NACE_4Dcode']
        }
    )

    selected_sub_bvd9_ids = pd.read_csv(files['OUTPUT']['SUBS']['BVD9_SHORT'],
                                        na_values='#N/A',
                                        header=None,
                                        dtype=str
                                        )[0]

    selected_sub_ids = sub_ids[sub_ids.sub_bvd9.isin(selected_sub_bvd9_ids)]

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
        na_values='#N/A',
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
            sub_ids,
            sub_fins
        )

    rpt.update(report, cases)
else:
    print('Read from files ...')

    parent_exposure = pd.read_csv(
        files['OUTPUT']['PARENTS']['EXPO'],
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9']
        }
    )

    sub_exposure = pd.read_csv(
        files['OUTPUT']['SUBS']['EXPO'],
        na_values='#N/A',
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
        parent_fins
    )

    rpt.update(report, cases)
else:
    print('Read from file ...')

    parent_rnd = pd.read_csv(
        files['OUTPUT']['PARENTS']['RND'],
        na_values='#N/A',
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
        na_values='#N/A',
        dtype={
            col: str for col in ['bvd9', 'sub_bvd9']
        }
    )
# </editor-fold>

# <editor-fold desc="#7 - Final reporting and consolidation">
print('#7 - Final reporting and consolidation')

# TODO: transfer in a specific report.py file and transfer report.py in method.py
# TODO: How does disclosed rnd and oprev in subs compare with rnd and oprev in parents
# TODO: How much disclosed sub rnd is embedded in keyword matching subs and not accounted for in subs final rnd
# TODO: How much disclosed parent rnd is embedded in parent that have a potential keyword match but have no subsidiaries
# TODO: Select top 10 parents in each cluster and consolidate oprev, exposure and rnd trends over range_ys
# TODO: Distribution and cumulative distribution functions for parent and subs (rnd x oprev or market cap?) by world_player
# TODO: Ex-post exposure global and by world_player

if not files['FINAL']['BY_APPROACH'].exists():
    
    print('Consolidate rnd by approach')

    rnd_conso = pd.DataFrame()

    print('> Prepare sub_rnd ...')

    categories = list(keywords.keys())

    rnd_cluster_cats = [cat for cat in categories if cat not in ['generation', 'rnd']]

    sub_rnd_grouped = rpt.merge_n_group_sub_rnd(
        cases,
        rnd_cluster_cats,
        sub_rnd.loc[:, ['sub_bvd9', 'bvd9', 'year', 'sub_rnd_clean', 'method']],  # sub_rnd['method'] == 'keep_subs'
        parent_ids.loc[:, ['guo_bvd9', 'bvd9', 'is_listed_company']],
        parent_guo_ids[['guo_bvd9', 'guo_type']],
        sub_ids[['sub_bvd9', 'sub_country_2DID_iso']].drop_duplicates(subset='sub_bvd9'),
        country_map,
        sub_fins
    )

    sub_rnd_grouped.rename(columns={
        'sub_country_3DID_iso': 'country_3DID_iso',
        'sub_world_player': 'world_player',
        'sub_rnd_clean': 'rnd_clean'
    }, inplace=True)

    print('> Prepare soeur_rnd ...')

    soeur_rnd_grouped = rpt.load_n_group_soeur_rnd(
        cases,
        files
    )

    soeur_rnd_grouped.rename(columns={
        'sub_country_3DID_iso': 'country_3DID_iso',
        'sub_world_player': 'world_player',
        'sub_rnd_clean': 'rnd_clean'
    }, inplace=True)

    print('> Prepare mnc_rnd ...')

    mnc_rnd_grouped = rpt.load_n_group_MNC_rnd(
        cases,
        files
    )

    mnc_rnd_grouped.rename(columns={
        'group_country_3DID_iso': 'country_3DID_iso',
        'group_world_player': 'world_player',
        'group_rnd_clean': 'rnd_clean'
    }, inplace=True)

    print('> Consolidated dataframe ...')

    rnd_conso_cols = ['approach', 'method', 'year', 'sub_rnd_clean', 'guo_type', 'type', 'sub_world_player',
                      'sub_country_3DID_iso', 'cluster', 'technology', 'priority', 'action']

    rnd_conso = rnd_conso.append(soeur_rnd_grouped)

    rnd_conso = rnd_conso.append(sub_rnd_grouped)

    rnd_conso = rnd_conso.append(mnc_rnd_grouped)

    print('> Re-group for tailored output table ...')

    rnd_conso = rnd_conso.groupby(['approach', 'year', 'world_player', 'country_3DID_iso']).sum()

    # Save output tables
    rnd_conso.to_csv(files['FINAL']['BY_APPROACH'],
                     columns=['rnd_clean'],
                     float_format='%.10f',
                     na_rep='#N/A'
                     )

# print('Consolidate rnd by MNC')

# rpt.get_group_rnd_distribution(
#     cases,
#     files,
#     keywords,
#     parent_ids,
#     parent_rnd,
#     sub_rnd_grouped_w_bvd9
# )

# with open(cases['CASE_ROOT'].joinpath(r'report.json'), 'r') as file:
#     report = json.load(file)
#
# rpt.pprint(report, cases)
# </editor-fold>


# sys.exit('My break')
