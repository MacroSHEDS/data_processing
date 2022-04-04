



all_fils <- list.files('data/czo/catalina_jemez/raw/precip_chemistry__5491/data_v2/',
                       full.names = TRUE)

all_dl <- tibble()
for(i in 1:length(all_fils)){
    if(i == 11) next
    one_year <- read.csv(all_fils[i], colClasses = 'character')
    one_year <- one_year[1:2,]
    one_year <- one_year %>%
        select(-DateTime, -SiteCode, -SampleCode, -contains('SamplingMethod'), -SampleType, -contains('pH'),
               -contains('EC'), -contains('SamplingNote'))
    
    year <- str_split_fixed(all_fils[i], '_', n = Inf)[,9]
    year <- substr(year, 1, 4)
    if(i == 10){
        year <- 2010
    }
    
    units <- one_year[1,]
    limits <- one_year[2,]
    
    dl_units <- pivot_longer(units, everything(), names_to = 'var', values_to = 'units') 
    limits <- pivot_longer(limits, everything(), names_to = 'var', values_to = 'detection_limit')
    
    fin <- full_join(limits, dl_units) %>%
        mutate(start_date = paste0('01-01-', year),
               end_date = paste0('31-12-', year))
    
    all_dl <- rbind(all_dl, fin)
}


data_cols =  c('pH', 'EC' = 'spCond',
               'TIC', 'TOC', 'TN', 'F', 'Cl', 'NO2',
               'Br', 'NO3', 'SO4', 'PO4', 'Ca', 'Mg',
               'Na', 'K', 'Sr', 'Si28' = 'Si', 'B', 'Be9' = 'Be',
               'Al27' = 'Al', 'Ti49', 'V51' = 'V',
               'Cr52' = 'Cr', 'Mn55' = 'Mn', 'Fe56' = 'Fe',
               'Co59' = 'Co', 'Ni60', 'Cu63' = 'Cu',
               'Zn64' = 'Zn', 'As75' = 'As', 'Se78',
               'Y89' = 'Y', 'Mo98' = 'Mo', 'Ag107' = 'Ag',
               'Cd114' = 'Cd', 'Sn118', 'Sb121' = 'Sb',
               'Ba138' = 'Ba', 'La139' = 'La', 'Ce140' = 'Ce',
               'Pr141' = 'Pr', 'Nd145', 'Sm147',
               'Eu153' = 'Eu', 'Gd157', 'Tb159' = 'Tb',
               'Dy164' = 'Dy', 'Ho165' = 'Ho', 'Er166' = 'Er',
               'Tm169' = 'Tm', 'Yb174' = 'Yb', 'Lu175' = 'Lu',
               'TI205' = 'Tl', 'Pb208' = 'Pb', 'U238' = 'U',
               'NH4.N' = 'NH4_N', 'Ca40' = 'Ca', 'Mg24' = 'Mg',
               'Na23' = 'Na', 'K39' = 'K', 'Sr88' = 'Sr', 'B11' = 'B',
               'F.' = 'F', 'Cl.' = 'Cl', 'NO2.' = 'NO2', 'Br.' = 'Br',
               'NO3.' = 'NO3', 'SO4.' = 'SO4', 'PO4.' = 'PO4',
               'Zr90' = 'Zr', 'Si', 'P31' = 'P', 'Ortho.P' = 'PO4_P',
               'Cd111' = 'Cd',
               'Ge74' = 'Ge', 'Nb93' = 'Nb', 'Tl205' = 'Tl', 'Fe',
               'Al', 'Ba', 'Hg202' = 'Hg')

var_cor <- tibble(var = names(data_cols),
                  new_var = unname(data_cols)) %>%
    mutate(var = ifelse(var == '', new_var, var)) 

all_dl <- all_dl %>%
    filter(var %in% !!var_cor$var) %>%
    left_join(., var_cor) %>%
    mutate(units = ifelse(units == 'umoles/L', 'umol/L', units))

write_csv(all_dl, 'stream_chemistry__4135.csv')
# nm


