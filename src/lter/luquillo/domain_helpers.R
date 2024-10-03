generate_column_cases <- function(pattern, replacement){
  expr <- expr(grepl(!!pattern, .x, ignore.case = TRUE) ~ sub(!!pattern, !!replacement, tolower(.x)))
  return(expr)
}

code_map <- data.frame(
	ptrn = paste0(c('chloride', 'nitrate', 'sulfate', 'sodium', 'potassium',
					'magnesium', 'calcium', 'nh4', 'po4'),
				  'code'),
	rplc = paste0(c('cl', 'no3.n', 'so4.s', 'na', 'k', 'mg', 'ca', 'nh4.n', 'po4.p'),
				  'code')
)
cases <- purrr::map2(code_map$ptrn, code_map$rplc, generate_column_cases)
cases[[length(cases) + 1]] <- expr(TRUE ~ .x)
