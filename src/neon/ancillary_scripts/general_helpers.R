get_siteprod = function(site_id, prod_path){

    prod = tibble()
    monthfiles = list.files(glue('{p}/{s}', p=prod_path, s=site_id))

    for(m in monthfiles){
        prod_part = read_feather(glue('{p}/{s}/{m}', p=prod_path, s=site_id, m=m))
        prod = bind_rows(prod, prod_part)
    }

    return(prod)
}
