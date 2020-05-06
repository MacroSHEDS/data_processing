#depth 0

prod_specs = get_neon_product_specs(neonprods$prodcode[i])
if(is_ms_err(prod_specs)){
    msg = 'NEON may have created a v.002 product. investigate!'
    email_err(msg, 'mjv22@duke.edu', conf$gmail_pw)
    stop(msg)
}

if(! product_is_tracked(held_data, prodname_ms)){
    stop(glue('Product {p} is not yet tracked. Retrieve ',
        'it before munging it.', p=prodname_ms))
}

avail_sets = sm(get_avail_neon_product_sets(prod_specs$prodcode_full))
if(is_ms_err(avail_sets)){
    email_err_msg = TRUE
    next
}

if(nrow(new_sets) == 0){
    logging::loginfo(glue('Nothing to do for {s} {n}',
        s=curr_site, n=prodname_ms), logger='neon.module')
    next
}

tryCatch({
    site_dset = get_neon_data(domain=domain, new_sets, held_data)
}, error=function(e){
    logging::logerror(e, logger='neon.module')
    email_err_msg <<- outer_loop_err <<- TRUE
})
if(outer_loop_err) next

#depth 1

if(is_ms_exception(out_sitemonth)){
    update_data_tracker_r(domain, tracker_name='held_data', set_details=s,
        new_status='error')
    next
} else if(is_ms_err(out_sitemonth)){
    update_data_tracker_r(domain, tracker_name='held_data', set_details=s,
        new_status='error')
    assign('email_err_msg', TRUE, pos=.GlobalEnv)
    next
}

#

msg = paste0('If tracker is not supplied, these must be:',
    'tracker_name, set_details, new_status.')
logging::logerror(msg, logger='neon.module')
stop(msg)
