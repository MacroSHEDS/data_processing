# dpID = 'DP1.20093.001' #wchem
# dpID = 'DP1.20288.001' #wqual
# dpID = 'DP1.20053.001' #temp
# dpID = 'DP1.20042.001' #PAR
# dpID = 'DP1.20033.001' #no3
# dpID = 'DP1.20097.001' #gas
# dpID = 'DP1.00006.001' #precip
# dpID = 'DP4.00130.001' #q
# dpID = 'DP1.00013.001' #pchem
# dpID = 'DP1.20206.001' #isotopes
# dpID = 'DP1.00038.001' #precip isotopes

neonUtilities::getTimeIndex(dpID)
dd = neonUtilities::loadByProduct(
    dpID = dpID,
    site = 'MAYF',
    startdate = '2022-01',
    enddate = '2022-01',
    package = 'basic',
    release = 'current',
    include.provisional = FALSE,
    # savepath = '/tmp/neon/schem',
    check.size = FALSE,
    # timeIndex = '30'
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
