macrosheds sites are organized by network (e.g. LTER), domain (e.g. HBEF), and site (e.g. W1)

the directory structure reflects this scheme, e.g.:
network1
    domain1
        site1
        site2
    domain2
        site1

this structure is duplicated for source files (contained in the "src" directory) and data files (in the "data") dir

at any level of this structure, files may exist that are specific to that level of specificity. for example, helper functions useful across all lter sites, but not useful for neon sites should be here: .../macrosheds/data_acquisition/src/lter/network_helpers.R

some networks include only one domain, e.g. NEON. For these, you will find a child directory with the same name as its parent

here's an pared down example that uses real names

data acquisition
    data
        general
            --data and metadata that apply to all networks
        lter
            hbef
            hjandrews
            ...
        neon
            neon
        ...
    src
        lter
            hbef
            hjandrews
            ...
        neon
            neon
        ...


        
