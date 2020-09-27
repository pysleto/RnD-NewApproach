from config import registry as reg

parent_ids_collection = ['bvd9', 'is_parent', 'is_GUO']

parent_ids = ['guo_bvd9', 'bvd9', 'company_name', 'parent_conso', 'bvd_id', 'legal_entity_id'] + \
             ['is_quoted', 'is_parent', 'is_GUO'] + \
             ['NACE_4Dcode', 'NACE_desc', 'subs_n'] + \
             ['country_2DID_iso'] + \
             ['guo_direct%']

guo_ids = ['guo_bvd9', 'guo_name', 'guo_conso', 'guo_bvd_id', 'guo_legal_entity_id'] + \
          ['is_quoted', 'is_parent', 'is_GUO'] + \
          ['NACE_4Dcode', 'NACE_desc'] + \
          ['guo_country_2DID_iso', 'guo_country_3DID_iso', 'guo_world_player']

parent_fins = ['bvd9', 'parent_conso', 'country_2DID_iso'] + \
              ['trade_desc', 'products_services_desc', 'full_overview_desc', 'bvd_sectors', 'main_activity_desc',
               'primary_business_line_desc'] + \
              ['Emp_number_LY', 'rnd_sum', 'oprev_sum'] + reg.rnd_ys[::-1] + reg.oprev_ys[::-1]

sub_ids = {'collection': ['bvd9', 'sub_bvd9', 'company_name', 'parent_conso', 'country_2DID_iso', 'sub_company_name',
                          'sub_country_2DID_iso', 'sub_lvl', 'sub_direct%', 'sub_total%']}

# sub_ids = ['bvd9', 'sub_bvd9', 'sub_company_name', 'sub_bvd_id', 'sub_legal_entity_id',
#            'sub_country_2DID_iso', 'sub_world_player', 'sub_NACE_4Dcode', 'sub_NACE_desc', 'sub_lvl', 'keep_all',
#            'keep_comps', 'keep_subs']

sub_fins = ['sub_bvd9', 'sub_conso', 'trade_desc', 'products&services_desc', 'full_overview_desc', 'rnd_sum',
            'oprev_sum'] + \
           reg.oprev_ys_for_exp[::-1] + \
           ['sub_turnover_sum', 'sub_turnover_sum_masked', 'keyword_mask'] + \
           [cat for cat in reg.categories]

parent_exp = ['bvd9', 'parent_name', 'parent_conso', 'keyword_mask_sum_in_parent',
              'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent', 'parent_exposure',
              'method']

sub_exp = ['bvd9', 'sub_bvd9', 'parent_name', 'sub_company_name', 'sub_conso', 'keyword_mask',
           'sub_turnover_sum', 'sub_turnover_sum_masked',
           'sub_exposure'] + parent_exp[1:]

parent_rnd = ['bvd9', 'parent_name', 'parent_conso', 'year', 'parent_oprev', 'parent_rnd', 'parent_exposure',
              'parent_rnd_clean', 'method']

guo_rnd = ['guo_bvd9', 'guo_name', 'guo_conso', 'guo_country_2DID_iso', 'guo_world_player', 'is_GUO', 'is_top_2000',
           'is_top_100',
           'year', 'guo_oprev_from_parent', 'guo_rnd_from_parent', 'guo_exposure_from_parent', 'guo_exposure',
           'guo_rnd_clean_from_parent', 'method']

sub_rnd = ['guo_bvd9', 'bvd9', 'sub_bvd9',
           'sub_company_name', 'sub_conso', 'sub_exposure', 'sub_country_2DID_iso', 'sub_world_player', 'sub_rnd_clean',
           'parent_name', 'parent_conso', 'parent_country_2DID_iso', 'parent_world_player',
           'guo_name', 'guo_conso', 'guo_country_2DID_iso', 'guo_world_player', 'is_top_2000', 'is_top_100',
           'year', 'method']

# TODO: define dtype of all variables above
dtype = {
    col: str for col in [
        'guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id',
        'bvd9', 'bvd_id', 'legal_entity_id', 'parent_conso', 'NACE_4Dcode',
        'parent_bvd9', 'parent_bvd_id', 'parent_legal_entity_id', 'parent_conso', 'parent_NACE_4Dcode', 'parent_conso',
        'parent_ticker', 'parent_ISIN',
        'sub_bvd9', 'sub_bvd_id', 'sub_legal_entity_id', 'sub_conso', 'sub_NACE_4Dcode'
    ]
}