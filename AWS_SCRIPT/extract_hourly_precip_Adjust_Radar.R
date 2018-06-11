
### Path to the folder AWS_DATA
AWS_DATA_DIR <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/AWS_DATA"

##############
## Start date (local time) to extract in the format "YYYY-MM-DD HH:MM:SS"
start_time <- "2018-01-01 00:00:00"
## End date (local time) to extract in the format "YYYY-MM-DD HH:MM:SS"
end_time <- "2018-01-20 00:00:00"

##########
# Note
# precipitation at 10 hr are the aggregated precipitation from 10:10:00 to 11:00:00

########################################## End Edit #############################################
cat("Extract hourly precipitation .........\n")

coord.dir <- file.path(AWS_DATA_DIR, "coordinates_files")
coords.LSI <- read.table(file.path(coord.dir, "LSI.csv"), header = TRUE, sep = ',', colClasses = 'character', stringsAsFactors = FALSE)
coords.REMA <- read.table(file.path(coord.dir, "REMA.csv"), header = TRUE, sep = ',', colClasses = 'character', stringsAsFactors = FALSE)
coords.LSI <- coords.LSI[, c(1, 3, 4)]
coords.LSI[, 2:3] <- apply(coords.LSI[, 2:3], 2, as.numeric)
coords.REMA <- coords.REMA[, c(1, 3, 4)]
coords.REMA[, 2:3] <- apply(coords.REMA[, 2:3], 2, as.numeric)

###################
start_time <- as.POSIXct(start_time, tz = "Africa/Kigali")
end_time <- as.POSIXct(end_time, tz = "Africa/Kigali")
daty <- seq(start_time, end_time, "hour")

precip <- lapply(c("LSI_AWS", "REMA_AWS"), function(AWS){
	AWS_DIR1hr <- file.path(AWS_DATA_DIR, AWS, "compressed_data", "data_1hr")
	awslistx <- list.files(AWS_DIR1hr, ".rds")
	awslist <- gsub("[^[:digit:]]", "", awslistx)
	prec <- lapply(awslist, function(aws){
		don <- readRDS(file.path(AWS_DIR1hr, paste0(aws, ".rds")))
		don.daty <-  as.POSIXct(strptime(don$date, "%Y%m%d%H", tz = "Africa/Kigali"))
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

csvfiles <- file.path(AWS_DATA_DIR, "AWS_1hr_Adjust_Radar", paste0("aws_", format(daty, "%Y%m%d_%H"), ".csv"))

for(j in seq_along(daty)){
	rema <- cbind(REM.crd, REM.don[j, ])
	lsi <- cbind(LSI.crd, LSI.don[j, ])
	don <- rbind(rema, lsi)
	write.table(don, csvfiles[j], sep = ",", col.names = FALSE, row.names = FALSE, quote = FALSE)
}
rm(precip, LSI.don, REM.don, lsi, rema, don); gc()
cat("Extracting hourly precipitation finished successfully\n")
