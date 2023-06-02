#TODO:
#+ i haven't tested the bit that's supposed to print unmatched items in
#   the "locations affected AND sensor positions" section (currently lines 181-187)

setwd('~/git/macrosheds/readme_wrangling/')

library(tidyverse)
library(glue)

d = read.csv('BLWA DO readme.csv') %>%
    pull(2)

changelog_start = grep('^CHANGE ?LOG$', d, ignore.case = TRUE) + 1
changelog_end = grep('^ADDITIONAL ? REMARKS$', d, ignore.case = TRUE) - 1

d = d[changelog_start:changelog_end]

#visually comb for variations on "Issue Date:", and "Resolution:",
#which will bookend all issue chunks (uncomment to chez)
# grep('issue', d, ignore.case = TRUE, value = TRUE)
# grep('issue ?date', d, ignore.case = TRUE, value = TRUE)
#
# grep('resolution', d, ignore.case = TRUE, value = TRUE)
# str_subset(d, regex('resolution ?date', ignore_case = TRUE), negate = TRUE) %>%
#     str_subset(regex('resolution', ignore_case = TRUE))
# #wow! some sort of consistency!

chunk_starts = grep('^issue ?date:', d, ignore.case = TRUE)
chunk_ends = grep('^resolution:', d, ignore.case = TRUE)
if(length(chunk_starts) != length(chunk_ends)) stop('hmm')

chunks = mapply(function(a, b) seq(a, b, 1),
                a = chunk_starts,
                b = chunk_ends)

date_regex = '[0-9]{4}-[0-9]{2}-[0-9]{2}'
base_rgx = 'Location\\(s\\) Affected: *'
site_rgx = '([A-Z]{4}) *'
sensorpos_rgx = '([0-9]{3}\\.?/?[0-9]{0,3})? *'
horver_rgx = '(?:HOR\\.VER[:\\.] ?)?{prgx}[;:/,]?(?:\\)[,;/: ]?\\()(?: ?and)? *'

output = tibble()
for(i in seq_along(chunks)){

    dc = d[chunks[[i]]]

    #issue date
    issue_date_ind = grep('Issue Date:', dc)
    issue_date = str_match(dc[issue_date_ind],
                           paste0('Issue Date: ?(',
                                  date_regex,
                                  ')$'))[, 2]
    if(is.na(issue_date)) stop('handle issue date variant')

    #issue
    issue_ind = grep('Issue:', dc)
    issue = str_match(dc[issue_ind],
                      'Issue: (.+)')[, 2]
    if(is.na(issue)) stop('handle issue variant')

    #date ranges
    date_range_inds = grep('Date Range:', dc)
    rgx = glue('Date Range: ?({dt}) ?(?:to|through|thru) ?({dt})$',
               dt = date_regex)
    tryCatch({
        date_ranges = str_match(dc[date_range_inds], rgx)[, 2:3, drop = FALSE]
        date_range_starts = date_ranges[, 1]
        date_range_ends = date_ranges[, 2]
    }, error = function(e) stop('handle date range variant'))

    #locations affected AND sensor positions
    loc_affected_inds = grep('Location\\(s\\) Affected:', dc)

    locs_affected = list()
    sensor_positions = list()

    rgx1 = paste0(base_rgx, 'all ?(?:sites|locations)?\\.?$')
    for(j in seq_along(loc_affected_inds)){
        rgx_match = grepl(rgx1, dc[loc_affected_inds[j]], ignore.case = TRUE)
        if(rgx_match){
            locs_affected[[j]] = 'all'
            sensor_positions[[j]] = NA_character_
        }
    }

    rgx2 = paste0(base_rgx, site_rgx, '$')
    for(j in seq_along(loc_affected_inds)){
        rgx_match = str_match(dc[loc_affected_inds[j]], rgx2)[, 2, drop = FALSE]
        if(! is.na(rgx_match)){
            locs_affected[[j]] = rgx_match
            sensor_positions[[j]] = NA_character_
        }
    }

    rgx3 = glue('{brgx}{srgx}\\((?:HOR\\.VER[:\\.] ?)?{prgx}\\)?$',
                brgx = base_rgx,
                srgx = site_rgx,
                prgx = sensorpos_rgx)
    for(j in seq_along(loc_affected_inds)){
        rgx_match = str_match(dc[loc_affected_inds[j]], rgx3)[, 2:3, drop = FALSE]
        if(! any(is.na(rgx_match))){
            locs_affected[[j]] = rgx_match[1]
            sensor_positions[[j]] = rgx_match[2]
        }
    }

    #note: there are some sensor positions that i think are missing
    #their final "0", like for index 171. We could replace those with the correct
    #posisions, assuming that final zero.
    rgx4 = glue('{brgx}{srgx}\\((?:HOR\\.VER[:\\.] ?)?{prgx}[;:/,]?(?: ?and)? *(?:HOR\\.VER[:\\.] ?)?{prgx}\\)?$',
                brgx = base_rgx,
                srgx = site_rgx,
                prgx = sensorpos_rgx)
    for(j in seq_along(loc_affected_inds)){
        rgx_match = str_match(dc[loc_affected_inds[j]], rgx4)[, 2:4, drop = FALSE]
        if(! any(is.na(rgx_match))){
            locs_affected[[j]] = rgx_match[1]
            sensor_positions[[j]] = rgx_match[2:3]
        }
    }

    rgx5 = glue('{brgx}{srgx}\\((?:HOR\\.VER[:\\.] ?)?{prgx}[;:/,]?(?: ?and)? *(?:HOR\\.VER[:\\.] ?)?{prgx}[;:/,]?{prgx}\\)?$',
                brgx = base_rgx,
                srgx = site_rgx,
                prgx = sensorpos_rgx)
    for(j in seq_along(loc_affected_inds)){
        rgx_match = str_match(dc[loc_affected_inds[j]], rgx5)[, 2:5, drop = FALSE]
        if(! any(is.na(rgx_match))){
            locs_affected[[j]] = rgx_match[1]
            sensor_positions[[j]] = rgx_match[2:4]
        }
    }

    rgx6 = paste0(base_rgx, 'all ?(?:sites|locations)?\\.?.+$')
    for(j in seq_along(loc_affected_inds)){
        rgx_match = grepl(rgx6, dc[loc_affected_inds[j]], ignore.case = TRUE)
        if(rgx_match){
            locs_affected[[j]] = dc[loc_affected_inds[j]]
            sensor_positions[[j]] = NA_character_
        }
    }

    #yargh!
    # rgx7 = glue('{brgx}{srgx}\\({hrgx}{hrgx}{hrgx}{hrgx}\\)?$',
    #             brgx = base_rgx,
    #             srgx = site_rgx,
    #             hrgx = glue(horver_rgx, prgx = sensorpos_rgx))
    # for(j in seq_along(loc_affected_inds)){
    #     rgx_match = str_match(dc[loc_affected_inds[j]], rgx7)[, 2:5, drop = FALSE]
    #     if(! any(is.na(rgx_match))){
    #         locs_affected[[j]] = rgx_match[1]
    #         sensor_positions[[j]] = rgx_match[2:4]
    #     }
    # }

    # #for testing: this lets you see how each change affects matches across every
    # #"Location(s) Affected:" chunk
    # zz = grep('Location', d, value = TRUE)
    # zz[which(sapply(locs_affected, function(x) is.null(x)))]

    locs_affected_v = sapply(locs_affected,
                             function(x) if(is.null(x)) return(NA_character_) else x)

    sensor_positions1 = vapply(sensor_positions, function(x)
        {
            res = try(x[1])
            if(inherits(res, 'try-error') || is.null(res)) res = NA_character_
            res
        },
        FUN.VALUE = 'a')

    sensor_positions2 = vapply(sensor_positions, function(x)
        {
            res = try(x[2])
            if(inherits(res, 'try-error') || is.null(res)) res = NA_character_
            res
        },
        FUN.VALUE = 'a')

    # if(any(is.null(locs_affected))){
    #     wonky_lines = dc[which(sapply(locs_affected, function(x) is.null(x)))]
    #     wonky_line_indices = chunks[[i]][which(dc %in% wonky_lines)] + changelog_start
    #     print(glue('gotta fix these lines:\n{l}',
    #                paste0(paste(wonky_line_indices, wonky_lines, sep = ':'),
    #                       '\n')))
    # }

    #resolution date
    resolution_date_ind = grep('Resolution Date:', dc)
    resolution_date = str_match(dc[resolution_date_ind],
                           paste0('Resolution Date: ?(',
                                  date_regex,
                                  ')$'))[, 2]

    #resolution
    resolution_ind = grep('Resolution:', dc)
    resolution = str_match(dc[resolution_ind],
                      'Resolution: (.+)')[, 2]

    #combine
    if(! length(locs_affected_v)){
        locs_affected_v = 'investigate'
    }
    if(! length(sensor_positions1)){
        sensor_positions1 = 'investigate'
    }
    if(! length(sensor_positions2)){
        sensor_positions2 = 'investigate'
    }

    output = bind_rows(output, tibble(issue_date = issue_date,
                                      issue = issue,
                                      date_range_starts = date_range_starts,
                                      date_range_ends = date_range_ends,
                                      locs_affected = locs_affected_v,
                                      sensor_pos_1 = sensor_positions1,
                                      sensor_pos_2 = sensor_positions2,
                                      resolution_date = resolution_date,
                                      resolution = resolution))
}

head(output)
