# process_1_VERSIONLESS001run doe/walker_branch process_1_VERSIONLESS001 to generate `d`

#obs vs interp
d[duplicated(d$datetime) | duplicated(d$datetime, fromLast = TRUE),] %>%
    arrange(datetime)

plot(interped$val, not_interped$val, xlim = c(0, 70), ylim = c(0, 70))
abline(a=0, b=1)
abline(lm(not_interped$val ~ interped$val))

#just the non-zeros
qz = cbind(interped = drop_errors(interped$val), not_interped = drop_errors(not_interped$val)) %>%
    as_tibble() %>%
    filter(interped != 0, not_interped != 0)

plot(qz$interped, qz$not_interped, xlim = c(0, 70), ylim = c(0, 70))
abline(a=0, b=1)
abline(lm(qz$not_interped ~ qz$interped))

#binary
qx = cbind(interped = drop_errors(interped$val), not_interped = drop_errors(not_interped$val)) %>%
    as_tibble() %>%
    mutate(interped = as.numeric(interped > 0),
           not_interped = as.numeric(not_interped > 0))

qxx = apply(qx, 1, sum)
#0 good, 2, good, 1 bad
table(qxx)
(214 + 211) / nrow(qx) * 100
#58% of interped precip days at least correctly identify whether there was precip or not. not very good.
