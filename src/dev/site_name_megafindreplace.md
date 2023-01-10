# change every instance of site_name to site_code
(repeat all actions in portal and data_acquisition repos)

0. Commit any changes with git, so there's a clean snapshot to return to if anything goes wrong!

1. find all filetypes in repo

   `find ~/git/macrosheds/portal -type f -name '*.*' | sed 's|.*\.||' | sort -u`
   `find ~/git/macrosheds/data_acquisition -type f -name '*.*' | sed 's|.*\.||' | sort -u`


2. grep those filetypes to find all the characters that might precede/succeed "site_name"

   `grep -roPh --include='*.R' '(.)site_name' ~/git/macrosheds/portal | sort -u`
   `grep -roPh --include='*.R' 'site_name(.)' ~/git/macrosheds/portal | sort -u`
   same for data_acqisition, other filetypes...
   
   ```
   $site_name
   nsite_name
    site_name
   !site_name
   "site_name
   #site_name
   'site_name
   (site_name
   -site_name
   <site_name
   =site_name
   [site_name
   _site_name
   ~site_name
   tsite_name
   
   site_name 
   site_name"
   site_name'
   site_name)
   site_name,
   site_name.
   site_name:
   site_name;
   site_name=
   site_name>
   site_name[
   site_name]
   site_name_
   site_names
   
   .site_name
   ,site_name
   {site_name
   site_name}
   ```
   
   I investigated all instances of \*site_name\* (with grep) that seemed like they could cause variable name collisions. same with site_code. all seems well, so this find-and-replace will be straightforward. the one hitch is that some raw data files have columns called site_name that we won't want to change (step 4).

3. which filetypes need amending?

   text filetypes to amend in data_acquisition:
       R, txt, csv, md, Rmd, sql

   text filetypes to amend in portal:
       R, txt, csv, html

   binary filetypes to amend in data_acquisition:
       feather, shp

   binary filetypes to amend in portal:
       feather, shp, fst


4. note which kernels refer to columns called "site_name" in raw data files

    <UPDATE> jk, these instances don't involve data files with "site_name" columns, but rather pre-processed
        data.frames with "site_name" columns, so they don't need to be manually put back as they were. also see \*\*\*

     #note any variations in how site_name_col arg is passed
     `grep -r --include='*.R' site_name_col`
     #totally uniform. cool. these files and lines will need revisiting:
     `grep -r --include='*.R' -n "site_name_col = 'site_name'"`

     ```
     src/lter/plum/processing_kernels.R:587:                             site_name_col = 'site_name',
     src/lter/plum/processing_kernels.R:613:                             site_name_col = 'site_name',
     src/lter/plum/processing_kernels.R:754:                         site_name_col = 'site_name',
     src/lter/plum/domain_helpers.R:53:                             site_name_col = 'site_name',
     src/lter/plum/domain_helpers.R:81:                             site_name_col = 'site_name',
     src/lter/plum/domain_helpers.R:133:                             site_name_col = 'site_name',
     src/lter/plum/domain_helpers.R:145:                             site_name_col = 'site_name',
     src/lter/plum/domain_helpers.R:195:                         site_name_col = 'site_name',
     src/lter/plum/domain_helpers.R:247:                         site_name_col = 'site_name',
     src/lter/plum/domain_helpers.R:299:                         site_name_col = 'site_name',
     src/lter/plum/domain_helpers.R:355:                         site_name_col = 'site_name',
     src/lter/plum/domain_helpers.R:397:                         site_name_col = 'site_name',
     src/lter/santa_barbara/domain_helpers.R:49:                             site_name_col = 'site_name',
     src/lter/santa_barbara/domain_helpers.R:59:                             site_name_col = 'site_name',
     src/lter/santa_barbara/domain_helpers.R:100:                         site_name_col = 'site_name',
     src/lter/arctic/processing_kernels.R:716:                             site_name_col = 'site_name',
     src/lter/arctic/processing_kernels.R:736:                             site_name_col = 'site_name',
     src/lter/arctic/processing_kernels.R:783:                         site_name_col = 'site_name',
     src/lter/arctic/processing_kernels.R:833:                         site_name_col = 'site_name',
     src/lter/arctic/processing_kernels.R:885:                         site_name_col = 'site_name',
     src/lter/arctic/OLD_processing_kernels.R:1219:                             site_name_col = 'site_name',
     src/lter/arctic/OLD_processing_kernels.R:1239:                             site_name_col = 'site_name',
     src/lter/arctic/domain_helpers.R:221:                             site_name_col = 'site_name',
     src/lter/arctic/domain_helpers.R:241:                             site_name_col = 'site_name',
     src/lter/arctic/domain_helpers.R:285:                             site_name_col = 'site_name',
     src/lter/arctic/domain_helpers.R:305:                             site_name_col = 'site_name',
     ```

5. modify text files (check with `git status` and `git diff` to make sure nothing unexpected is happening)

   `cd ~/git/macrosheds/portal; find . -regextype posix-extended -regex '.*\.(R|txt|csv|html)' | xargs sed -e 's/site_name/site_code/g' -i`
   #couldn't read two files with spaces in their names, but manual investigation of those files shows nothing to be done
   `cd ~/git/macrosheds/data_acquisition; find . ! -path './data/*/raw/*' -regextype posix-extended -regex '.*\.(R|txt|csv|md|Rmd|sql)' | xargs sed -e 's/site_name/site_code/g' -i`
   
6. go back and manually replace "site_name" in the lines from section 4 (\*\*\* jk. this isn't needed. keeping these sections here so we remember to think about this in the event of a similar global search and replace)
7. modify binary files (see `data_acquisition/src/dev/update_header_names_globally.R`)
8. modify remote config files
9. change "description" column for site_code in watershed_summaries_metadata.csv so that it reads "short name of the site"
