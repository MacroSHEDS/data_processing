# MacroSheds Changelog

Dataset version 2

2024-10-01

## Improvements

 + NEON stream and river sites are now included in MacroSheds! See Miscellaneous section below for notes.
 + Other new domains:
   + Sleepers River Research Watershed
   + Panola Mountain Research Watershed
   + Loch Vale
   + Trout Lake Station LTER
 + Annual solute load estimates are now included, using the methods described in [Gubbins et al. (in review)](https://eartharxiv.org/repository/view/6513/).
   + Period-weighting (linear interpolation)
   + Rating (AKA regression model method)
   + Composite
   + Beale ratio estimator
   + Average
   + a "MacroSheds recommended" method for each site-year, determined using the methods described in [Aulenbach et al. 2016](https://esajournals.onlinelibrary.wiley.com/doi/full/10.1002/ecs2.1298).
 + New timeseries variables: tritium, humification index, water color, specific UV absorbance, delta-duterium, orthophosphate, turbidity NTU vs. FNU
 + New watershed attributes: global MODIS GPP and NPP, enhanced vegetation index (EVI)
 + Known disturbance record for all sites now included.
 + Cleaned up output data format:
   + MacroSheds was originally designed to accommodate sub-daily time series, but that plan has been indefinitely sidelined. As such, we now report dates instead of datetimes for each observation.
   + Sampling regimen (i.e. grab versus sensor) is now a separate column, rather than a variable prefix, and the 4-way distinction (I, G, S, N) has been simplified to just "grab sample: TRUE or FALSE".
   + Variable prefixes for watershed attributes are no more. Variable category and source are now distinct columns in the metadata.
 + Now using StreamStats watershed delineation wherever a USGS gauge coincides with a MacroSheds site
 + Improved accuracy of aggregation of sub-daily to daily discharge observations by using a modified version of the trapezoidal rule.

## Bugfixes

 + Bugs related to the summarizing of watershed attributes:
   + For watersheds smaller than 10 hectares, Google Earth Engine (GEE) reducers sometimes fail to produce a value. These small watershed boundaries have been replaced with a 10 ha circle for the purposes of GEE reductions, resulting in precip_median, temp_median, LAI, NDVI, and other spatial summary values where previously they were missing. Affected sites include weir_4 (calhoun), MCDN (baltimore), and several others.
   + For watershed attributes available through GEE, processing resolution is now determined on a per-site basis, rather than a per-domain basis.
   + Some partial retrievals from GEE went unnoticed in v1 (e.g. luquillo NDVI, many east_river variables). These are now temporally complete.
   + Fixed scaling issues with some GEE products that report a scale factor. These went unnoticed in v1, resulting in illegal values for NDVI and LAI, and unreasonable values for CONUS GPP and NPP, and fPAR.
 + Precip-discharge imbalance still occurs for some sites, primarily those influenced by permanent snow/ice or highly unpredictable subsurface flowpaths, but many P-Q imbalance issues have been resolved.
 + Fixed a QC-code assimilation bug that was allowing a small number of "bad data" values into the final product. These may have appeared as outliers in v1.
 + Fixed a QC-code assimilation bug that resulted in a small number of good data values being removed. This fix preserves 3% of hjandrews chemistry and 3% of mcmurdo chemistry data that were lost in v1.
 + Several timezones were incorrectly labeled as UTC instead of local, or vice-versa. These have been fixed, resulting in small changes to some daily values.
   + Rarely, a datetime processing bug could result in each value of a chemistry, discharge, or precipitation time series being shifted by one day. This is no longer possible.
 + Fixed incorrect precipitation totals for calhoun
 + Fixed bug in precipitation spatial interpolator that was replacing some 0 values with NA.
 + Some reported detection limits (DLs) were being lost during processing, resulting in incorrectly assumed 1/2 DLs being inserted in place of true 1/2 DLs. This has been fixed.
 + Located several missing provenance records (notably, for all USGS-derived discharge).
 + Diverse micro-fixes, e.g.:
   + NaN values and duplicates should no longer appear in any output files.
   + Removed derelict "year" column that was showing up in some discharge files.

## Miscellaneous

 + NEON notes
   + neon reports BDL and ND. We give BDL values a flag. ND gets replaced with DL/2.
   + Where "domain lab" and "external lab" data both present, these are averaged (e.g. swc_domainLabData and swc_externalLabDataByAnalyte for stream chemistry 20093)
   + For domain lab data, ms_statusof 1 is assigned if remarks are present and not related to "replicate", "SOP", "protocol", or "cartridge"
   + For external lab data,
     + shipmentWarmQF flag yields ms_status of 1, 
     + prioritize sampleCondition == GOOD, else take OK, else settle for Other
   + NEON discharge series are supplemented by composite discharge series from [Vlah et al. 2024](https://hess.copernicus.org/articles/28/545/2024/). All simulated values are marked by ms_status = 1 and ms_interp = 1. All original NEON values, if kept, are marked 0 (clean).		
   + Ignoring upstream sensor array (S1) readings UNLESS a value is missing from S2. PAR is not borrowed from upstream if downsteram missing.
 + Previously, if there were duplicate datetime-site_code pairs in a raw data file, we would keep the row with the fewest missing values. now we take the mean for each variable, omitting missing values.
 + Consolidated time-series variables that were synonymous or commensurable, e.g. TP and P, TDP and FTP, pH and acidity.
 + Implemented two-phase interpolation for precipitation. True missing values (e.g. gauge goes offline) are interpolated from NLDAS/PRISM where possible, then range-checked, then interpolated with 0s or mean-nocb, depending on the way raw data are reported.
 + Implemented two-phase interpolation for precip chemistry. Up to two months of true missing values (e.g. gauge goes offline) are linearly interpolated for known precip events. Then, precip chemisty is range-checked and BDL values are replaced with 1/2 detection limit. Finally, series are zero- or nocb-interpolated depending on the presence of corresponding precipitation data and way raw values are reported.
 + No longer reporting concentrations of e.g. nitrate and nitrate-N. Only the latter is reported, and the former can be easily computed using tools from the "macrosheds" package.
 + located additional precip gauge sites at hjandrews, konza, hbef
 + Krycklan precip chemistry has been dropped from the MacroSheds dataset due to low temporal coverage of public data.
 + Located several missing watershed attribute descriptions, and made language more precise for all watershed attribute descriptions.
 + Changed site_codes for east_river to match those reported in primary source data.
