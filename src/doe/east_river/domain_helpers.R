east_river_sites_of_interest <- site_data %>%
    filter(domain == 'east_river',
           site_type != 'rain_gauge') %>%
    pull(site_code) %>%
    tolower()

east_river_error_codes <- c('-9999', '-4999.5', '4999.467', '4999.500',
                            '4999.498', '4999.495')

east_river_site_name_map = c(
    'ER_AVY1' = 'Avery',
    'ER_PHF0' = 'PH',
    # 'ER_EBC1' = '',
    'ER_CPR1' = 'Copper',
    # 'ER_BTH1' = '',
    'ER_MMT1' = 'Marmot',
    'ER_GTH1' = 'Gothic',
    'ER_RCK1' = 'Rock',
    'ER_BRD1' = 'Bradley',
    'ER_RUS1' = 'Rustlers',
    'ER_EAQ1' = 'EAQ',
    'ER_QLY1' = 'Quigley'
    # 'ER_BLY1' = '',
    # 'COAL_11' = '',
    # 'ER_TTL1' = ''
    #WG and OBJ are in different basins
)

east_river_anions <- c('chloride', 'fluoride', 'nitrate', 'nitrite',
                       'phosphate', 'sulfate')

east_river_cations <- c(
    'aluminum' = 'Al',
    'antimony' = 'Sb',
    'arsenic' = 'As',
    'barium' = 'Ba',
    'beryllium' = 'Be',
    'boron' = 'B',
    'cadmium' = 'Cd',
    'calcium' = 'Ca',
    'cesium' = 'Cs',
    'chromium' = 'Cr',
    'cobalt' = 'Co',
    'copper' = 'Cu',
    'europium' = 'Eu',
    'germanium' = 'Ge',
    'iron' = 'Fe',
    'lead' = 'Pb',
    'lithium' = 'Li',
    'magnesium' = 'Mg',
    'manganese' = 'Mn',
    'molybdenum' = 'Mo',
    'nickel' = 'Ni',
    'phosphorus' = 'TP',
    'potassium' = 'K',
    'rubidium' = 'Rb',
    'selenium' = 'Se',
    'silicon' = 'Si',
    'silver' = 'Ag',
    'sodium' = 'Na',
    'strontium' = 'Sr',
    'thorium' = 'Th',
    'tin' = 'Sn',
    'titanium' = 'Ti',
    'uranium' = 'U',
    'vanadium' = 'V',
    'zinc' = 'Zn',
    'zirconium' = 'Zr'
)
