from front_end import *

db=ProjectDataBase("Predis","Presquile")
db.dbFullfillerFromCSV(['globalConsuption', 'data_Predis_02_02_2017/Td SC-BT-586-ARM-BAT-F02-TD2_DEM_40-TGBT_2.csv'],
                       ['globalProduction', 'data_Predis_02_02_2017/Td SC-BT-838-ARM-BAT-F40-INJ_photovolt-TGBT_2.csv'])

