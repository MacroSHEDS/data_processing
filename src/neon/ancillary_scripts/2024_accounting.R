# dpID = 'DP1.20093.001'
dpID = 'DP1.20288.001'
# dpID =
neonUtilities::getTimeIndex(dpID)
dd = neonUtilities::loadByProduct(
    dpID = dpID,
    site = set_details$site_code,
    startdate = '2023-01',
    enddate = '2023-01',
    package = 'basic',
    release = 'current',
    include.provisional = FALSE,
    # savepath = '/tmp/neon/schem',
    check.size = FALSE
    # timeIndex = '100'
)

#schem
d0 = dd
d0v = sort(d0$swc_externalLabDataByAnalyte$analyte)
'ANC, Br, Ca, Cl, CO3, DIC, DOC, F, Fe, HCO3, K, Mg, Mn, Na, NH4 - N, NO2 - N,
NO3+NO2 - N, Ortho - P, Si, SO4, specificConductance, TDN, TDP, TDS, TN, TOC,
TP, TPC, TPC, TPN, TPN, TSS, TSS - Dry Mass, UV Absorbance (254 nm), UV Absorbance (280 nm)'
#wqual
d1 = dd
d1$waq_instantaneous$startDateTime
'specificConductance, dissolvedOxygen, seaLevelDissolvedOxygenSat, localDissolvedOxygenSat,
pH, chlorophyll, chlaRelativeFluorescence, turbidity, fDOM, rawCalibratedfDOM'
