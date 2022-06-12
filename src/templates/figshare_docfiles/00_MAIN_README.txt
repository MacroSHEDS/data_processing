Thanks for downloading the MacroSheds dataset.

There's a lot of documentation here. We don't expect you to read everything, but PLEASE
carefully consider our Data Use Agreements. Most of the data included here are (re)distributed
under CC0 (no rights reserved), CC BY 4.0 (attribution required), or equivalents, but a few of our
primary sources have additional intellectual rights attached to their data.
Details are in 01a_data_use_agreements.docx, alongside this readme. For citations, DOIs, etc., broken down
by domain and product, see 01b_attribution_and_intellectual_rights_complete.xlsx.
And please contact us (mail@macrosheds.org) if you feel any MacroSheds content should be amended.

For a thorough description of the methods involved in building this dataset, see the
corresponding publication (pending). Also note that the easiest way to explore
our data catalog is use the interactive catalogs on macrosheds.org (Data tab).
You can also visualize the dataset there.

Otherwise, just refer to this file, and the many others it names, as needed. And don't forget
about the "macrosheds" R package, which helps with retrieving and working with MacroSheds data
(https://github.com/MacroSHEDS/macrosheds). All project code is available
on GitHub: https://github.com/MacroSHEDS

---

MacroSheds dataset contents:

NOTE: in addition to the files in 0_documentation_and_metadata, there are
individual "documentation" directories in 2_timeseries_data/<n>/<d>,
where <n> is any MacroSheds network and <d> is any MacroSheds domain within
<n>. These additional documentation directories contain information on dataset
retrieval locations, and code used to retrieve, munge, and derive each
timeseries data product.

0_documentation_and_metadata (this directory)
├── 01a_data_use_agreements.docx
├── 01b_attribution_and_intellectual_rights_complete.xlsx
├── 02_glossary.txt
├── 03_changelog.txt
├── 04_site_documentation
│   ├── 04a_site_metadata.csv
│   └── 04b_site_metadata_column_descriptions.txt
├── 05_timeseries_documentation
│   ├── 05b_timeseries_variable_metadata.csv
│   ├── 05c_timeseries_variable_metadata_column_descriptions.txt
│   ├── 05d_timeseries_column_descriptions.txt
│   ├── 05e_range_check_limits.csv
│   ├── 05f_detection_limits_and_precision.csv
│   ├── 05g_detection_limits_and_precision_column_descriptions.txt
│   └── 05h_timeseries_refs.bib
├── 06_ws_attr_documentation
│   ├── 06b_ws_attr_variable_metadata.csv
│   ├── 06c_ws_attr_variable_metadata_column_descriptions.txt
│   ├── 06d_ws_attr_variable_category_codes.csv
│   ├── 06e_ws_attr_data_source_codes.csv
│   ├── 06f_ws_attr_summary_column_descriptions.csv
│   ├── 06g_ws_attr_timeseries_column_descriptions.txt
│   └── 06h_ws_attr_refs.bib
├── 07_CAMELS-compliant_datasets_documentation
│   ├── 07a_CAMELS-compliant_datasets_metadata.txt
│   ├── 07b_CAMELS-compliant_ws_attributes_column_descriptions.txt
│   └── 07c_CAMELS-compliant_Daymet_forcings_column_descriptions.txt
└── 08_data_irregularities.csv

1_watershed_attribute_data
├── ws_attr_summaries.csv
└── ws_attr_timeseries
    ├── climate.csv
    ├── hydrology.csv
    ├── landcover.csv
    ├── parentmaterial.csv
    ├── terrain.csv
    └── vegetation.csv

2_timeseries_data
└── separate directories for each MacroSheds network
    └── separate directories for each MacroSheds domain within that network
        ├── documentation (of retrieval locations and code used to retrieve/munge/derive each product)
        ├── discharge (if available) 
        ├── precipitation (if available)
        ├── stream_chemistry (if available)
        ├── precip chemistry (if available)
        ├── stream_gauge_locations
        ├── precip_gauge_locations (if available)
        └── ws_boundary (except McMurdo)

3_CAMELS-compliant_watershed_attributes
├── clim.csv
├── geol.csv
├── soil.csv
├── topo.csv
└── vege.csv

4_CAMELS-compliant_Daymet_forcings
└── forcings for each site in MacroSheds

