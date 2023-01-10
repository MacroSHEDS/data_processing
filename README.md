# data_acquisition directory structure

+ macrosheds sites are organized by network (e.g. LTER), domain (e.g. HBEF), and site (e.g. W1)
+ the directory structure reflects this scheme, e.g.:

  network1  
  &nbsp;&nbsp;domain1  
  &nbsp;&nbsp;&nbsp;&nbsp;site1  
  &nbsp;&nbsp;&nbsp;&nbsp;site2  
  &nbsp;&nbsp;domain2  
  &nbsp;&nbsp;&nbsp;&nbsp;site1  

+ this structure is duplicated for source files (contained in the "src" directory) and data files (in the "data") dir
+ at any level of this structure, files may exist that are specific to that level of specificity. for example, helper functions useful across all lter sites, but not useful for neon sites, should be here: .../macrosheds/data_acquisition/src/lter/network_helpers.R
+ some networks include only one domain, e.g. NEON. For these, you will find a child directory with the same name as its parent
+ here's a pared down example that uses real names

  data acquisition  
  &nbsp;&nbsp;data  
  &nbsp;&nbsp;&nbsp;&nbsp;general  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;--data and metadata that apply to all networks  
  &nbsp;&nbsp;&nbsp;&nbsp;lter  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;hbef  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;hjandrews  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;...  
  &nbsp;&nbsp;&nbsp;&nbsp;neon  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;neon  
  &nbsp;&nbsp;&nbsp;&nbsp;...  
  &nbsp;&nbsp;src  
  &nbsp;&nbsp;&nbsp;&nbsp;lter  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;hbef  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;hjandrews  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;...  
  &nbsp;&nbsp;&nbsp;&nbsp;neon  
  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;neon  
  &nbsp;&nbsp;&nbsp;&nbsp;...  

# new network/domain acquisition SOP

**this is a work in progress. update as we go, so these steps generalize**

**note:**
we have three data levels within macrosheds. these are used in naming processing kernels.
+ 0 = raw (exactly as retrieved from the source, either saved in the retrieval format (e.g. CSV, DAT) or saved in our own format (feather)
+ 1 = munged (ready for the macrosheds portal)
+ 2 = derived (calculated from level 1 data; also ready for the macrosheds portal)
So, for example, the 1 in process_1_20093 means this is a munge kernel.

**begin SOP:**
1. for a new domain or new network, research it's naming conventions. fill out name_variants.csv with a new column
2. you might already have a retrieve.R script for this network/domain. if not, borrow one from some already handled network/domain. use this as a template
3. step through ms superstructure until processing can't proceed successfully without modification
    1. if error/incompatibility occurs in retrieve.R modify retrieve.R to accommodate new idiosyncrasies
        + mind existing retrieve.R scripts as you develop. attempt to harmonize accross them
        + add, remove, modify helper functions and/or the body of retrieve.R in order to accomplish this
        + ideally, one day, retrieve.R will be flexible enough to handle all network-domains. then it can be a single function in a macrosheds package
    2. if error/incompatibility occurs in the outer retrieval function (e.g. get_neon_data, get_lter_data), do as above.
        + e.g. modify get_networkx_data to accommodate new idiosyncrasies. try to harmonize it with get_neon_data and get_lter_data as much as possible
    3. borrow a retrieval kernel (the innermost level of retrieval processing, where the data are actually downloaded) to use as a template. if it doesn't work as-is, modify it
4. you might already have a munge.R script for this network/domain. if not, borrow one from some already handled network/domain. use this as a template
    1. repeat above instructions for munge.R
    2. repeat as above for munge function, e.g. munge_neon_site()
    3. error/incompatibility _should_ occur in the munge kernel, the innermost level of munge processing
        + here's where you need to accommodate somewhat arbitrary data structures in whatever form they arrive. within domains, we can expect good consistency. within networks, some. between networks, anything goes.
        + there will be errors in the external database. build to accommodate unforseen issues:
            + naming inconsistencies
            + missing data
            + incorrectly specified data
            + unexpected columns, etc.
        + make sure datetime is converted to UTC (lubridate::with_tz and lubridate::force_tz are great for this)
        + make sure variable units are converted to those specified in data/general/variables.csv
        + make sure column names are converted to the ones we use in ms
            + "site_code"
            + "datetime"
            + e.g. "spCond" (again, consult variables.csv for variable names)
        + if necessary, separate one site into multiple sites. we have to do this for some neon "sites", where there's secretly an upstream sensor array and a downstream sensor array.
5. we haven't started deriving data yet, but the process will be similar to the above. build out this section when we get there

