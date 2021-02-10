
#retrieval kernels ####

#discharge: STATUS=READY
#. handle_errors
process_0_2504 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_6686 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_2644 <- function(set_details, network, domain) {

    dir.create(glue('data/{n}/{d}/raw/{p}/sitename_NA/{c}',
                    d = domain,
                    n = network,
                    p = set_details$prodname_ms,
                    c = str_split_fixed(set_details$component, '[/]', n = Inf)[1,1]),
               recursive = T)

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_0_2497 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_4135 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#stream_chemistry: STATUS=READY
#. handle_errors
process_0_2740 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2532 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2491 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2494 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2531 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2475 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precipitation: STATUS=READY
#. handle_errors
process_0_2543 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_5491 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#precip_chemistry: STATUS=READY
#. handle_errors
process_0_5492 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#munge kernels ####

#discharge: STATUS=READY
#. handle_errors
process_1_2504 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_1_2504 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_1_6686 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_1_2644 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#discharge: STATUS=READY
#. handle_errors
process_1_2497 <- function(set_details, network, domain) {

    download_raw_file(network = network,
                      domain = domain,
                      set_details = set_details,
                      file_type = NULL)

    return()
}

#derive kernels ####