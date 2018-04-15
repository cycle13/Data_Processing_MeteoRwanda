
### Path to the folder AWS_DATA
AWS_DATA_DIR <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/AWS_DATA"

##############
## Start date to extract in the format "YYYY-MM-DD"
start_time <- "2018-01-01"
## End date to extract in the format "YYYY-MM-DD"
end_time <- "2018-01-10"

##########
# Note
# precipitation at 2014-01-23 are computed using hourly data from 2014-01-23 08:00:00 to 2014-01-24 07:00:00

########################################## End Edit #############################################
cat("Extract daily precipitation .........\n")

coord.dir <- file.path(AWS_DATA_DIR, "coordinates_files")
coords.LSI <- read.table(file.path(coord.dir, "LSI.csv"), header = TRUE, sep = ',', colClasses = 'character', stringsAsFactors = FALSE)
coords.REMA <- read.table(file.path(coord.dir, "REMA.csv"), header = TRUE, sep = ',', colClasses = 'character', stringsAsFactors = FALSE)
coords.LSI <- coords.LSI[, c(1, 3, 4)]
coords.LSI[, 2:3] <- apply(coords.LSI[, 2:3], 2, as.numeric)
coords.REMA <- coords.REMA[, c(1, 3, 4)]
coords.REMA[, 2:3] <- apply(coords.REMA[, 2:3], 2, as.numeric)

###################
start_time <- as.Date(start_time)
end_time <- as.Date(end_time)
daty <- seq(start_time, end_time, "day")

precip <- lapply(c("LSI_AWS", "REMA_AWS"), function(AWS){
	AWS_DIR1day <- file.path(AWS_DATA_DIR, AWS, "compressed_data", "data_daily")
	awslistx <- list.files(AWS_DIR1day, ".rds")
	awslist <- gsub("[^[:digit:]]", "", awslistx)
	prec <- lapply(awslist, function(aws){
		don <- readRDS(file.path(AWS_DIR1day, paste0(aws, ".rds")))
		don.daty <-  as.Date(don$date.RR, "%Y%m%d")
		don$RR[match(daty, don.daty), 1]
	})
	list(stn = awslist, rr = do.call(cbind, prec))
})

LSI.stn <- precip[[1]]$stn
LSI.don <- precip[[1]]$rr
LSI.ix <- match(LSI.stn, coords.LSI$id)
LSI.crd <- as.matrix(coords.LSI[LSI.ix, ])
LSI.crd <- LSI.crd[!is.na(LSI.ix), ]
LSI.don <- LSI.don[, !is.na(LSI.ix)]

REM.stn <- precip[[2]]$stn
REM.don <- precip[[2]]$rr
REM.ix <- match(REM.stn, coords.REMA$id)
REM.crd <- as.matrix(coords.REMA[REM.ix, ])
REM.crd <- REM.crd[!is.na(REM.ix), ]
REM.don <- REM.don[, !is.na(REM.ix)]

precip <- cbind(REM.don, LSI.don)
precip[is.na(precip)] <- -99
xhead <- t(rbind(REM.crd, LSI.crd))
daty <- format(daty, "%Y%m%d")
capt <- c("AWS_ID", "LON", "DATE/LAT")
precip <- rbind(cbind(capt, xhead), cbind(daty, precip))

csvfile <- file.path(AWS_DATA_DIR, "AWS_1day_CDT_Station_Format",
					paste0("rr_daily_", daty[1], "_to_", daty[length(daty)], ".csv"))
write.table(precip, csvfile, sep = ",", col.names = FALSE, row.names = FALSE, quote = FALSE)

rm(precip, LSI.don, REM.don); gc()
cat("Extracting daily precipitation finished successfully\n")
