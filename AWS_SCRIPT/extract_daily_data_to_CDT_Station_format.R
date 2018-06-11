
### Path to the folder AWS_DATA
AWS_DATA_DIR <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/AWS_DATA"

##############
## variable to extract
# Rainfall: "RR"
# Min Temperature: "TN"
# Mean Temperature: "TM"
# Max Temperature: "TX"

variable <- "RR"

##############
## Start date to extract in the format "YYYY-MM-DD"
start_time <- "2015-01-01"
## End date to extract in the format "YYYY-MM-DD"
end_time <- "2017-12-31"

##########
# Note
# precipitation at 2014-01-23 are computed using hourly data from 2014-01-23 08:00:00 to 2014-01-24 07:00:00

########################################## End Edit #############################################
cat("Extract daily AWS data .........\n")

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

###################

data.aws <- lapply(c("LSI_AWS", "REMA_AWS"), function(AWS){
	AWS_DIR1day <- file.path(AWS_DATA_DIR, AWS, "compressed_data", "data_daily")
	awslistx <- list.files(AWS_DIR1day, ".rds")
	awslist <- gsub("[^[:digit:]]", "", awslistx)
	dat <- lapply(awslist, function(aws){
		don <- readRDS(file.path(AWS_DIR1day, paste0(aws, ".rds")))
		don.daty <- if(variable == "RR") "date.RR" else "date.VAR"
		if(is.null(don[[don.daty]])) return(NA)
		don.daty <- as.Date(don[[don.daty]], "%Y%m%d")
		xx <- switch(variable, 
				"RR" = don$RR[, 1],
				"TN" = don$TT[, 1],
				"TM" = don$TT[, 2],
				"TX" = don$TT[, 3])
		xx[match(daty, don.daty)]
	})
	list(stn = awslist, data = do.call(cbind, dat))
})

LSI.stn <- data.aws[[1]]$stn
LSI.don <- data.aws[[1]]$data
LSI.ix <- match(LSI.stn, coords.LSI$id)
LSI.crd <- as.matrix(coords.LSI[LSI.ix, , drop = FALSE])
LSI.crd <- LSI.crd[!is.na(LSI.ix), , drop = FALSE]
LSI.don <- LSI.don[, !is.na(LSI.ix), drop = FALSE]
LSI.ix <- colSums(!is.na(LSI.don)) > 0
LSI.crd <- LSI.crd[LSI.ix, , drop = FALSE]
LSI.don <- LSI.don[, LSI.ix, drop = FALSE]

REM.stn <- data.aws[[2]]$stn
REM.don <- data.aws[[2]]$data
REM.ix <- match(REM.stn, coords.REMA$id)
REM.crd <- as.matrix(coords.REMA[REM.ix, , drop = FALSE])
REM.crd <- REM.crd[!is.na(REM.ix), , drop = FALSE]
REM.don <- REM.don[, !is.na(REM.ix), drop = FALSE]
REM.ix <- colSums(!is.na(REM.don)) > 0
REM.crd <- REM.crd[REM.ix, , drop = FALSE]
REM.don <- REM.don[, REM.ix, drop = FALSE]

data.aws <- cbind(REM.don, LSI.don)
data.aws[is.na(data.aws)] <- -99
xhead <- t(rbind(REM.crd, LSI.crd))
daty <- format(daty, "%Y%m%d")
capt <- c("AWS_ID", "LON", "DATE/LAT")
data.aws <- rbind(cbind(capt, xhead), cbind(daty, data.aws))

csvfile <- file.path(AWS_DATA_DIR, "AWS_1day_CDT_Station_Format",
			paste0(variable, "_daily_", daty[1], "_to_", daty[length(daty)], ".csv"))
write.table(data.aws, csvfile, sep = ",", col.names = FALSE, row.names = FALSE, quote = FALSE)

rm(data.aws, LSI.don, REM.don); gc()
cat("Extracting daily data finished successfully\n")
