from config import registry as reg

ids = {
    'collection':
        {'parent':
             ['company_name', 'bvd9', 'quoted', 'parent_conso', 'parent_ticker', 'parent_ISIN', 'bvd_id',
              'legal_entity_id', 'country_2DID_iso', 'NACE_4Dcode', 'NACE_desc', 'subs_n', 'guo_type', 'guo_name',
              'guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id', 'guo_country_2DID_iso', 'guo_direct%'],
         'sub':
             ['company_name', 'bvd9', 'parent_conso', 'country_2DID_iso', 'sub_bvd9', 'sub_company_name',
              'sub_country_2DID_iso', 'sub_lvl', 'sub_direct%', 'sub_total%']
         },
    'consolidation':
        {'parent':
             ['company_name', 'bvd9', 'quoted', 'parent_conso', 'parent_ticker', 'parent_ISIN', 'bvd_id',
              'legal_entity_id', 'country_2DID_iso', 'NACE_4Dcode', 'NACE_desc', 'subs_n', 'guo_type', 'guo_name',
              'guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id', 'guo_country_2DID_iso', 'guo_direct%'],
         'sub':
             ['sub_company_name', 'sub_bvd9', 'quoted', 'sub_conso', 'sub_ticker', 'sub_ISIN', 'sub_bvd_id',
              'sub_legal_entity_id', 'sub_country_2DID_iso', 'sub_NACE_4Dcode', 'sub_NACE_desc', 'subs_n', 'guo_type',
              'guo_name', 'guo_bvd9', 'guo_bvd_id', 'guo_legal_entity_id', 'guo_country_2DID_iso', 'guo_direct%']
         }
}
