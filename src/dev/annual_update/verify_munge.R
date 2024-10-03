#after re-munge, look for old files lying around ####
system("find . -path '*/munged/*.feather' -mtime +2", intern = TRUE)

#make sure everything has uncertainty attached (and is consistent) ####
mf = system("find . -path '*/munged/*.feather'", intern = TRUE)
for(f in mf){

    #plum and catalina are good. ms_interp and val_err get appended during derive
    if(str_split_fixed(f, '/', 5)[4] %in% c('plum', 'catalina_jemez')) next
    zz = feather::feather_metadata(f)
    gud = identical(setNames(c('datetime', 'character', 'character', 'double', 'double', 'double', 'double'),
                             c('datetime', 'site_code', 'var', 'val', 'ms_status', 'ms_interp', 'val_err')),
                    zz$types)
    if(! gud) print(f)
}

# feather::feather_metadata("./data/lter/plum/munged/discharge__397/saw_mill_brook.feather")

# remove sites that we haven't recorded in site_data ####

#NOT A LONG-TERM SOLUTION! some of these sites are probably legit (e.g. W101 at hbef).
#any non-legits should be handled in the kernel, in ms_read_csv? not sure about that, actually.
#they could become legit, and then we'd never know.

all_known_sites <- site_data$site_code

for(f in mf){
    site_code <- str_extract(f, '([^/]+)\\.feather', group = 1)
    if(! site_code %in% all_known_sites){
        print(f)
        #uncomment to remove files
        # file.remove(f)
    }
}
