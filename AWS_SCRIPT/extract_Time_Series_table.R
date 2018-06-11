
### Path to the folder AWS_DATA
AWS_DATA_DIR <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/AWS_DATA"
### Path to the folder AWS_SCRIPT
AWS_SCRIPT_DIR <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/AWS_SCRIPT"

### File to save the table in CSV format
table.csv <- "~/Desktop/Daily_Rainfall.csv"

########################################## End Edit #############################################
library(stringr)
library(jsonlite)

plt_pars <- file.path(AWS_SCRIPT_DIR, 'params', 'extract_time_series.json')
plt_pars <- fromJSON(plt_pars)
source(file.path(AWS_SCRIPT_DIR, "getAWS_TimeSeries.R"))

don <- rbind(c("DATE", STN.NAME), cbind(daty0, DATA.AWS))
don[is.na(don)] <- ""
write.table(don, table.csv, sep = ",", col.names = FALSE, row.names = FALSE, quote = FALSE)

