

### Path to the folder AWS_DATA
AWS_DATA_DIR <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/AWS_DATA"
### Path to the folder AWS_SCRIPT
AWS_SCRIPT_DIR <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/AWS_SCRIPT"

### File to save the table in CSV format
table.csv <- "~/Desktop/Spatial_Daily_Rainfall.csv"

########################################## End Edit #############################################
library(stringr)
library(jsonlite)

plt_pars <- file.path(AWS_SCRIPT_DIR, 'params', 'extract_spatial_table.json')
plt_pars <- fromJSON(plt_pars)
source(file.path(AWS_SCRIPT_DIR, "getAWS_SpatialData.R"))

AWS.DAT[is.na(AWS.DAT)] <- ""
write.table(AWS.DAT, table.csv, sep = ",", col.names = TRUE, row.names = FALSE, quote = FALSE)

