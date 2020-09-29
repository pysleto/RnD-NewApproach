from config import registry as reg


comp_ids = {
    'collection': {
        'parent':
            ['guo_bvd9', 'bvd9'],
        'sub':
            ['bvd9', 'sub_bvd9', 'company_name', 'parent_conso', 'country_2DID_iso', 'sub_company_name',
             'sub_country_2DID_iso', 'sub_lvl']
    },
    'consolidation': {
        'parent':
            ['guo_bvd9', 'bvd9', 'company_name', 'parent_conso', 'bvd_id', 'legal_entity_id'] + \
            ['is_quoted'] + \
            ['NACE_4Dcode', 'NACE_desc', 'subs_n'] + \
            ['country_2DID_iso'] + \
            ['guo_direct%'],
        'guo':
            ['guo_bvd9', 'guo_name', 'guo_country_2DID_iso'],
        'sub':
            ['sub_bvd9', 'sub_company_name', 'sub_conso', 'sub_bvd_id', 'sub_legal_entity_id'] + \
            ['is_quoted'] + \
            ['sub_NACE_4Dcode', 'sub_NACE_desc'] + \
            ['sub_country_2DID_iso', 'sub_world_player'],
        'sub_ext':
            ['parent_unique_id', 'country_2DID_iso', 'bvd9', 'parent_conso', 'company_name'] +
            ['sub_unique_id', 'sub_country_2DID_iso', 'sub_bvd9', 'sub_conso', 'sub_company_name', 'sub_bvd_id',
             'sub_legal_entity_id', 'sub_world_player', 'sub_NACE_4Dcode', 'sub_NACE_desc'] +
            ['sub_lvl', 'keep_all', 'keep_comps', 'keep_subs']
    }
}
comp_fins = {
    'parent': ['bvd9', 'parent_conso', 'country_2DID_iso'] + \
              ['trade_desc', 'products_services_desc', 'full_overview_desc', 'bvd_sectors', 'main_activity_desc',
               'primary_business_line_desc'] + \
              ['Emp_number_LY', 'rnd_sum', 'oprev_sum'] + reg.rnd_ys[::-1] + reg.oprev_ys[::-1],
    'sub': ['sub_bvd9', 'sub_conso', 'sub_country_2DID_iso'] + \
           ['trade_desc', 'products_services_desc', 'full_overview_desc', 'bvd_sectors', 'main_activity_desc',
            'primary_business_line_desc'] + \
           ['Emp_number_LY', 'rnd_sum', 'oprev_sum'] + reg.rnd_ys[::-1] + reg.oprev_ys[::-1]
}

parent_exp = ['bvd9', 'parent_name', 'parent_conso', 'parent_country_2DID_iso', 'parent_world_player',
              'keyword_mask_sum_in_parent', 'total_sub_turnover_sum_masked_in_parent',
              'total_sub_turnover_sum_in_parent', 'parent_exposure', 'method']

sub_exp = ['bvd9', 'sub_bvd9', 'parent_name', 'sub_company_name', 'sub_conso', 'sub_country_2DID_iso',
           'sub_world_player', 'keyword_mask', 'sub_turnover_sum', 'sub_turnover_sum_masked',
           'sub_exposure'] + parent_exp[2:]

# comp_exp = {
#     'parent': ['bvd9', 'parent_name', 'parent_conso', 'keyword_mask_sum_in_parent',
#                'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent', 'parent_exposure',
#                'method'],
#     'sub': ['bvd9', 'sub_bvd9', 'parent_name', 'sub_company_name', 'sub_conso', 'keyword_mask',
#             'sub_turnover_sum', 'sub_turnover_sum_masked',
#             'sub_exposure'] + parent_exp[1:]
#}

parent_rnd = ['bvd9', 'parent_name', 'parent_conso', 'parent_country_2DID_iso', 'parent_world_player', 'year',
              'parent_oprev', 'parent_rnd', 'parent_exposure', 'parent_rnd_clean', 'method']

guo_rnd = ['guo_bvd9', 'guo_name', 'guo_conso', 'guo_country_2DID_iso', 'guo_world_player', 'is_GUO', 'is_top_2000',
           'is_top_100',
           'year', 'guo_oprev_from_parent', 'guo_rnd_from_parent', 'guo_exposure_from_parent', 'guo_exposure',
           'guo_rnd_clean_from_parent', 'method']

sub_rnd = ['bvd9', 'sub_bvd9',
           'sub_company_name', 'sub_conso', 'sub_exposure', 'sub_country_2DID_iso', 'sub_world_player', 'sub_rnd_clean',
           'parent_name', 'parent_conso', 'parent_country_2DID_iso', 'parent_world_player',
           # 'guo_name', 'guo_country_2DID_iso', 'guo_world_player',
           'year', 'method']

# TODO: define dtype of all variables above
dtype = {
    **{col: str for col in [
        'guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id',
        'bvd9', 'bvd_id', 'legal_entity_id', 'parent_conso', 'NACE_4Dcode', 'company_name', 'country_2DID_iso',
        'parent_bvd9', 'parent_bvd_id', 'parent_legal_entity_id', 'parent_conso', 'parent_NACE_4Dcode', 'parent_conso',
        'parent_ticker', 'parent_ISIN',
        'sub_bvd9', 'sub_company_name', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_conso', 'sub_NACE_4Dcode',
        'sub_country_2DID_iso'
    ]},
    # **{col: int for col in ['sub_lvl']},
    # **{col: float for col in ['sub_direct%', 'sub_total%']}
}
