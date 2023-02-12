For MacroSheds processing methods, see the
<a href="https://eartharxiv.org/repository/view/3499/" style="color: blue; text-decoration: underline;">data paper preprint</a>, 
where you will find subheadings on time-series data processing, watershed attributes retrieval and processing,
derivation of additional products, and technical validation.

For field data collection methods, laboratory methods and protocols, and primary source quality control procedures,
consult primary source documentation. Links can be found in `attribution_and_intellectual_rights_ws_attr.csv` and `attribution_and_intellectual_rights_ts.xlsx`.

---

The subset of MacroSheds that relates to streamflow and climate forcings makes it a valuable supplement to existing datasets like
<a href="https://ral.ucar.edu/solutions/products/camels" style="color: vlue; text-decoration: underline;">CAMELS</a> (Newman et al. 2014) and <a href="https://water.usgs.gov/GIS/metadata/usgswrd/XML/gagesII_Sept2011.xml" style="color: vlue; text-decoration: underline;">GAGES-II</a>  (Falcone 2011).
Using CAMELS methods, we have compiled watershed attributes and Daymet forcings, for each MacroSheds site, that are commensurable
with the published CAMELS dataset, enhancing the predictive power of the combined set, especially for small watersheds. Of the 181
sites with discharge data that MacroSheds adds to this corpus (as of version 1), 122 have watershed areas of 10 km2 or less, and
68 have areas of 1 km2 or less. For CAMELS, these numbers are 8 and 0, respectively. For GAGES-II, they are 207 and 2
(see Figure 2 in the MacroSheds data paper).

Please note that we used gSSURGO (Soil Survey Staff 2022) instead of the superseded STATSGO dataset for soil characteristics.
Two other CAMELS watershed attributes, `pet_mean` (mean potential evapotranspiration) and aridity, were also computed
differently for MacroSheds watersheds. For these, we solved the Priestly-Taylor formulation by using a gridded _alpha_ product
(Aschonitis et al. 2017), rather than calibrating _alpha_ ourselves. In addition to the pet\_mean watershed summary variable in
`CAMELS_compliant_ws_attr_summaries.csv`, we have included timeseries of pet in `CAMELS_compliant_Daymet_forcings.csv`, though
pet is not a Daymet variable per se.

Hydrologic attributes (q\_mean, runoff\_ratio, stream\_elas, slope\_fdc, baseflow\_index\_landson, hfd\_mean, Q5, Q95,
high\_q\_freq, high\_q\_dur, low\_q\_freq, low\_q\_dur, zero\_q\_freq) were only computed for site-years with >= 85%
temporal coverage (i.e. with a measurement or estimate of Q and or P on at least 85% of days), except for polar/arid sites
at which full years of flow are never recorded. At these sites, >= 50% maximum coverage (i.e. at least half the number of days
as the most complete year ever recorded) were required.

The following CAMELS watershed attributes are not included in the MacroSheds dataset, but see related or analogous variables
(listed in parentheses) in `ws_attr_timeseries.csv`:

 1. lai\_max (vb\_lai\_median)
 1. lai\_diff
 1. gvf\_max (vb\_fpar\_median, vb\_ndvi\_median)
 1. gvf\_diff
 1. soil\_porosity (pm\_sub\_surf\_porosity\_mean)
 1. soil\_conductivity (pf\_soil\_cat\_exchange\_7)
 1. max\_water\_content
 1. water\_frac (pf\_soil\_water\_0bar, pf\_soil\_water\_0.1bar pf\_soil\_water\_0.33bar, pf\_soil\_water\_15bar)
 1. other\_frac

---

 + Aschonitis, VG et al. (2017): High-resolution global grids of revised Priestley-Taylor and Hargreaves-Samani coefficients for assessing ASCE-standardized reference crop evapotranspiration and solar radiation. Earth System Science Data, 9(2), 615-638, https://doi.org/10.5194/essd-9-615-2017. PANGAEA. https://doi.org/10.1594/PANGAEA.868808
 + Falcone, J. A. (2011). GAGES-II: Geospatial attributes of gages for evaluating streamflow. US Geological Survey. 
 + Newman, A., Sampson, K., Clark, M., Bock, A., Viger, R., & Blodgett, D. (2014). A large-sample watershed-scale hydrometeorological dataset for the contiguous USA. UCAR/NCAR, Doi, 10, D6MW2F4D. 
 + Soil Survey Staff. (2022). National Value Added Look Up (valu) Table Database for the Gridded Soil Survey Geographic (gSSURGO) Database for the United States of America and the Territories, Commonwealths, and Island Nations served by the USDA-NRCS. United States Department of Agriculture, Natural Resources Conservation Service. https://gdg.sc.egov.usda.gov/

