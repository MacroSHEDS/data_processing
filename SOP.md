# MacroSheds Developer Guide

## data_acquisition

we're always going to convert NO3, PO4, NH3, NH4, SiO2, SO4 to their main elements in moles, e.g. NO3 -> NO3-N
    if a domain provides both NO3 and NO3-N, we'll keep both

when to make a new function:
    when you paste the same code >= 3 times and only change a few things (nearly always)
    when you paste the same code 2 times and only change a few things (if convenient)
    when there's a decent chance we'll do the same thing again down the road
    
when to adapt an existing function:
    when there's a decent chance we'll be able to leverage that adaptation down the road
    this almost always applies when you're in the munge pipeline

ms000 is a reserved name for wbs that we delineate

after munge: ALL CLEANING-TYPE STUFF SHOULD BE DONE
    assume that if you're in a derive kernel:
        there are no duplicates, empty rows, local timezones, etc., and that all ms_statuses and ms_interps are in place
derive kernels must always be site-agnostic, i.e. they should operate on site_code='sitename_NA'
    this way, invalidate_derived_products will work as-is and... probably other stuff will be more convenient
precip stuff can be adjusted to the nearest day; everything else to the nearest hour

derive step involves products that might not be supplied by source. give these prodcodes e.g. ms001, ms002.
    must be "msXXX" where XXX is 3-digit 0-padded integer
sometimes a derive kernel will need to wait for another derive kernel to complete before it can run
    i.e. if an input product for the derivation is itself a derived product.
    use prodcodes to ensure the proper order, recognizing that prodcodes are alphanumerically sorted at the top of each
    derive script. so, if stream_flux needs stream_chem and discharge to run, and all are derived products, code them like so:
        ms001: stream_chem; ms002: discharge; ms003stream_flux
we want to keep basin average precip calculated by sources; but also calculate our own in a standard way
    same with discharge
    for now, just store these as precipitation_ns and discharge_ns; we'll work out how to viz them later
detection limits must be retrieved at the start of every munge kernel and reapplied at the end of it
kernels must be named process\_1\_\_\<prodcode\>. code should not include site name
reserved prodcodes:
    precipitation: ms900
    precip chem: ms901
    precip flux: ms902
only error catching at the kernel/engine levels
    we might want to restore some #. handle_errors eventually, but for now it's useful to know exactly where errors are being handled.
    hanging ue()
    return()



## portal

## Other
