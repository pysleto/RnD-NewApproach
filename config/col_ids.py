from config import registry as reg

parent_ids = ['bvd9', 'company_name', 'parent_conso', 'bvd_id', 'legal_entity_id', 'guo_bvd9'] + \
             ['is_' + str(company_type) for company_type in reg.company_types] + \
             ['NACE_4Dcode', 'NACE_desc', 'subs_n'] + \
             ['country_2DID_iso']

guo_ids = ['guo_bvd9', 'guo_type', 'guo_name', 'guo_conso', 'guo_bvd_id', 'guo_legal_entity_id', 'guo_country_2DID_iso']

parent_fins = ['bvd9', 'parent_conso', 'Emp_number_y' + reg.LY, 'sales_y' + reg.LY,
               'rnd_sum', 'oprev_sum'] + reg.rnd_ys[::-1] + reg.oprev_ys[::-1]


sub_ids = ['sub_bvd9', 'bvd9', 'sub_company_name', 'sub_bvd_id', 'sub_legal_entity_id',
           'sub_country_2DID_iso', 'sub_NACE_4Dcode', 'sub_NACE_desc', 'sub_lvl', 'keep_all', 'keep_comps',
           'keep_subs']

sub_fins = ['sub_bvd9', 'sub_conso', 'trade_desc', 'products&services_desc', 'full_overview_desc', 'rnd_sum',
            'oprev_sum'] + \
           reg.oprev_ys_for_exp[::-1] + \
           ['sub_turnover_sum', 'sub_turnover_sum_masked', 'keyword_mask'] + \
           [cat for cat in reg.categories]

parent_exp = ['bvd9', 'parent_conso', 'total_sub_turnover_sum_masked_in_parent', 'total_sub_turnover_sum_in_parent',
               'parent_exposure', 'method']

sub_exp = ['sub_bvd9', 'sub_conso', 'sub_turnover_sum', 'sub_turnover_sum_masked', 'sub_exposure'] + parent_exp

parent_rnd = ['bvd9', 'parent_conso', 'year', 'parent_oprev', 'parent_rnd', 'parent_exposure', 'parent_rnd_clean',
              'method']

sub_rnd = ['sub_bvd9', 'sub_conso', 'sub_exposure', 'is_sub_top', 'sub_rnd_clean',
           'bvd9', 'parent_conso', 'parent_exposure_from_sub', 'is_parent_top', 'parent_rnd_clean',
           'is_in_top', 'year', 'method']

# TODO: define dtype of all variables above
dtype = {
    col: str for col in [
        'guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id',
        'bvd9', 'bvd_id', 'legal_entity_id', 'parent_conso', 'NACE_4Dcode',
        'sub_bvd_id', 'sub_legal_entity_id', 'sub_conso', 'sub_NACE_4Dcode'
    ]
}
