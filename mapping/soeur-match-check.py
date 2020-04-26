import os
import sys

from pathlib import Path
import pandas as pd

import input

# Set  dataframe display options
pd.options.display.max_columns = None
pd.options.display.width = None

root = Path(os.getcwd())

output = r'working-files\group_ids-table.csv'

working_table_cols = [
    'soeur_group_name',
    'FP_bvd_company_name',
    'FP_bvd_id',
    'NewApp_company_name',
    'step',
    'comment',
    'FP_bvd_match_name',
    'FP_bvd_match_rate',
    'ratio_match_name',
    'ratio_match_rate',
    'partial_ratio_match_name',
    'partial_ratio_match_rate',
    'sort_ratio_match_name',
    'sort_ratio_match_rate',
    'set_ratio_match_name',
    'set_ratio_match_rate'
]

if not root.joinpath(output).exists():
    working_table = input.load_working_table_xls(root, 'working-files/MNC_table_working.xlsx')

    working_table.set_index('soeur_group_name', inplace=True)

    working_table.to_csv(
        root.joinpath(output),
        columns=working_table_cols[1:],
        float_format='%.10f',
        na_rep='#N/A'
    )

working_table = input.load_working_table_csv(root, output)

# <editor-fold desc="#00 - No match as group">
print('step #00')

print('Input: ' + str(working_table.index.value_counts().sum()))

# All match names are equal and match rate sum is maxed
step_query = working_table['step'].isna()

name_query = (working_table['FP_bvd_company_name'].isna()) & (working_table['FP_bvd_match_name'].isna()) & (
    working_table['ratio_match_name'].isna()) & (working_table['partial_ratio_match_name'].isna()) & (
                 working_table['sort_ratio_match_name'].isna()) & (working_table['set_ratio_match_name'].isna())

table_update = working_table[step_query & name_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'step'] = '#00'
table_update.loc[:, 'comment'] = 'No match as group - match with SBD or try as sub'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>

# <editor-fold desc="#01 - Previously identified, all match names are equal and match rate sum is maxed">
print('step #01')

print('Input: ' + str(working_table.index.value_counts().sum()))

# Previously identified all match names are equal and match rate sum is maxed
step_query = working_table['step'].isna()

name_query = working_table[
    ['FP_bvd_match_name', 'ratio_match_name', 'partial_ratio_match_name', 'sort_ratio_match_name',
     'set_ratio_match_name']].eq(working_table.loc[:, 'FP_bvd_match_name'], axis=0).all(1)

rate_query = working_table[
                 ['FP_bvd_match_rate', 'ratio_match_rate', 'partial_ratio_match_rate', 'sort_ratio_match_rate',
                  'set_ratio_match_rate']].sum(axis=1) == 500

table_update = working_table[step_query & name_query & rate_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['FP_bvd_match_name']
table_update.loc[:, 'step'] = '#01'
table_update.loc[:, 'comment'] = 'Perform random check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>

# <editor-fold desc="#02 - Not previously identified but all other match names are equal and match rate sum is maxed">
print('step #02')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# Not previously identified but all other match names are equal and match rate sum is maxed

name_query = working_table[
    ['ratio_match_name', 'partial_ratio_match_name', 'sort_ratio_match_name',
     'set_ratio_match_name']].eq(working_table.loc[:, 'ratio_match_name'], axis=0).all(1)

rate_query = working_table[
                 ['ratio_match_rate', 'partial_ratio_match_rate', 'sort_ratio_match_rate',
                  'set_ratio_match_rate']].sum(axis=1) == 400

table_update = working_table[step_query & name_query & rate_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['ratio_match_name']
table_update.loc[:, 'step'] = '#02'
table_update.loc[:, 'comment'] = 'Perform random check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>

# <editor-fold desc="#03 - Not previously identified, match names are equal, partial + set ratios are maxed and ratio + sort ratios > 80">
print('step #03')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# Not previously identified, match names are equal, partial + set ratios are maxed and ratio + sort ratios > 80

name_query = working_table[
    ['ratio_match_name', 'partial_ratio_match_name', 'sort_ratio_match_name',
     'set_ratio_match_name']].eq(working_table.loc[:, 'ratio_match_name'], axis=0).all(1)

rate_query = (working_table['ratio_match_rate'].ge(80)) & (working_table['partial_ratio_match_rate'].eq(100)) & (
    working_table['sort_ratio_match_rate'].ge(80)) & (working_table['set_ratio_match_rate'].eq(100))

table_update = working_table[step_query & name_query & rate_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['ratio_match_name']
table_update.loc[:, 'step'] = '#03'
table_update.loc[:, 'comment'] = 'Perform random check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>

# <editor-fold desc="#04 - Not previously identified, partial and set ratios are maxed and corresponding names are matching">
print('step #04')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# Not previously identified, partial and set ratios are maxed and corresponding names are matching

name_query = working_table['partial_ratio_match_name'] == working_table['set_ratio_match_name']

rate_query = (working_table['partial_ratio_match_rate'].eq(100)) & (working_table['set_ratio_match_rate'].eq(100))

table_update = working_table[step_query & name_query & rate_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['partial_ratio_match_name']
table_update.loc[:, 'step'] = '#04'
table_update.loc[:, 'comment'] = 'Perform random check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>

# <editor-fold desc="#05 - Previously identified, ratio + sort ratios > 80 with corresponding names are matching">
print('step #05')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# Previously identified, ratio + sort ratios > 80 with corresponding names are matching

name_query = working_table[
    ['ratio_match_name', 'sort_ratio_match_name']].eq(working_table.loc[:, 'FP_bvd_match_name'], axis=0).all(1)

rate_query = (working_table['FP_bvd_match_rate'].eq(100)) & (working_table['ratio_match_rate'].ge(80)) & (
    working_table['sort_ratio_match_rate'].ge(80))

table_update = working_table[step_query & name_query & rate_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['FP_bvd_match_name']
table_update.loc[:, 'step'] = '#05'
table_update.loc[:, 'comment'] = 'Perform random check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>

# <editor-fold desc="#06 - Not previously identified, set ratios = 100 AND other names are #N/A or matching">

print('step #06')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# Not previously identified, set ratios = 100 AND other names are #N/A or matching

rate_query = working_table['set_ratio_match_rate'].eq(100)

table_update = working_table[step_query & rate_query]

table_update['ratio_match_name'].mask(
    table_update['ratio_match_name'].isna(), table_update['set_ratio_match_name'], inplace=True
)
table_update['partial_ratio_match_name'].mask(
    table_update['partial_ratio_match_name'].isna(), table_update['set_ratio_match_name'], inplace=True
)
table_update['sort_ratio_match_name'].mask(
    table_update['sort_ratio_match_name'].isna(), table_update['set_ratio_match_name'], inplace=True
)

table_update = table_update[
    table_update[
        ['ratio_match_name', 'partial_ratio_match_name', 'set_ratio_match_name']].eq(
        table_update.loc[:, 'sort_ratio_match_name'], axis=0).all(1)
]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['sort_ratio_match_name']
table_update.loc[:, 'step'] = '#06'
table_update.loc[:, 'comment'] = 'Perform manual check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>

# <editor-fold desc="#07 - Previously identified with a matching set ratio = 100">
print('step #07')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# Previously identified with a matching set ratio = 100

rate_query = working_table['set_ratio_match_rate'].eq(100)

name_query = working_table['set_ratio_match_name'] == working_table['FP_bvd_match_name']

table_update = working_table[step_query & rate_query & name_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['FP_bvd_match_name']
table_update.loc[:, 'step'] = '#07'
table_update.loc[:, 'comment'] = 'Perform random check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>

# <editor-fold desc="#08 - Previously identified and matching at least one of the ratios">
print('step #08.1')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# if FP_bvd_match_name matches ratio_match_name

name_query = working_table['FP_bvd_match_name'] == working_table['ratio_match_name']

table_update = working_table[step_query & name_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['FP_bvd_match_name']
table_update.loc[:, 'step'] = '#08.1'
table_update.loc[:, 'comment'] = 'Perform manual check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)

print('step #08.2')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# if FP_bvd_match_name matches partial_ratio_match_name

name_query = working_table['FP_bvd_match_name'] == working_table['partial_ratio_match_name']

table_update = working_table[step_query & name_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['FP_bvd_match_name']
table_update.loc[:, 'step'] = '#08.2'
table_update.loc[:, 'comment'] = 'Perform manual check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)

print('step #08.3')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# if FP_bvd_match_name matches sort_ratio_match_name

name_query = working_table['FP_bvd_match_name'] == working_table['sort_ratio_match_name']

table_update = working_table[step_query & name_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['FP_bvd_match_name']
table_update.loc[:, 'step'] = '#08.3'
table_update.loc[:, 'comment'] = 'Perform manual check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)

print('step #08.4')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# if FP_bvd_match_name matches set_ratio_match_name

name_query = working_table['FP_bvd_match_name'] == working_table['set_ratio_match_name']

table_update = working_table[step_query & name_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'NewApp_company_name'] = table_update['set_ratio_match_name']
table_update.loc[:, 'step'] = '#08.4'
table_update.loc[:, 'comment'] = 'Perform manual check'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>

# TODO: query sub ids and allocate parent_bvd_id. For the rest, query ORBIS and identify corresponding parent
# <editor-fold desc="#09 - Previously identified but with no match name">
print('step #09')

step_query = working_table['step'].isna()

print('Input: ' + str(working_table[step_query].index.value_counts().sum()))

# Previously identified but with no match name

name_query = ~working_table['FP_bvd_company_name'].isna()

table_update = working_table[step_query & name_query]

print('Table update: ' + str(table_update.index.value_counts().sum()))

table_update.loc[:, 'step'] = '#09'
table_update.loc[:, 'comment'] = 'Check ownership in ORBIS, then attempt matching with subsidiary_ids'

working_table = input.update_working_table(root, table_update[['NewApp_company_name', 'step', 'comment']], output)
# </editor-fold>