
dir_dat <- switch(str_trim(plt_pars$Tstep),
				"10min" = "data_10minQC",
				"1hr" = "data_1hr",
				"1day" = "data_daily",
				"10day" = "data_dekad")

format.date <- switch(str_trim(plt_pars$Tstep),
				"10min" = "%Y%m%d%H%M%S",
				"1hr" = "%Y%m%d%H",
				"1day" = "%Y%m%d",
				"10day" = "%Y%m%d")

format.limit <- switch(str_trim(plt_pars$Tstep),
				"10min" = "%Y%m%d%H%M",
				"1hr" = "%Y%m%d%H",
				"1day" = "%Y%m%d",
				"10day" = "%Y%m%d")

coord.dir <- file.path(AWS_DATA_DIR, "coordinates_files")
coords.LSI <- read.table(file.path(coord.dir, "LSI.csv"), header = TRUE, sep = ',', colClasses = 'character', stringsAsFactors = FALSE)
coords.REMA <- read.table(file.path(coord.dir, "REMA.csv"), header = TRUE, sep = ',', colClasses = 'character', stringsAsFactors = FALSE)

LSI.STN <- file.path(AWS_DATA_DIR, "LSI_AWS", "compressed_data", dir_dat, paste0(coords.LSI$id, ".rds"))
lsi.exst <- file.exists(LSI.STN)
coords.LSI <- coords.LSI[lsi.exst, , drop = FALSE]
LSI.STN <- LSI.STN[lsi.exst]

REMA.STN <- file.path(AWS_DATA_DIR, "REMA_AWS", "compressed_data", dir_dat, paste0(coords.REMA$id, ".rds"))
rema.exst <- file.exists(REMA.STN)
coords.REMA <- coords.REMA[rema.exst, , drop = FALSE]
REMA.STN <- REMA.STN[rema.exst]

AWS.STN <- c(LSI.STN, REMA.STN)
AWS.CRD <- rbind(coords.LSI[, 1:4], coords.REMA[, 1:4])
rm(coords.LSI, coords.REMA)

AWS.DAT <- lapply(AWS.STN, function(pth){
	don <- readRDS(pth)
	if(str_trim(plt_pars$var) == "RR"){
		if(str_trim(plt_pars$Tstep)%in%c("1day", "10day")){
			daty <- don$date.RR
			vals <- don$RR[, 1]
		}else{
			daty <- don$date
			if(str_trim(plt_pars$Tstep) == "10min"){
				vals <- if(AWS == "LSI") don$data$RR$Tot else don$data$RR
			}else vals <- don$RR[, 1]
		}
	}else{
		if(str_trim(plt_pars$Tstep) == "10min"){
			nom.var <- names(don$data)
			nom.var <- nom.var[nom.var%in%c("TT", "TT2")]
			if(length(nom.var) == 0) return(NULL)
			if(length(nom.var) == 2) nom.var <- "TT"
			daty <- don$date
			if(AWS == "LSI"){
				idx <- switch(str_trim(plt_pars$var), "TN" = "Min", "TM" = "Ave", "TX" = "Max")
				vals <- don$data[[nom.var]][[idx]]
			}else{
				if(str_trim(plt_pars$var)%in%c("TN", "TX")) return(NULL)
				vals <- don$data[[nom.var]]
			}
		}else if(str_trim(plt_pars$Tstep) == "10day"){
			nom.var <- names(don)
			if(!"TMP"%in%nom.var) return(NULL)
			daty <- don$date.TMP
			idx <- switch(str_trim(plt_pars$var), "TN" = 1, "TM" = 2, "TX" = 3)
			vals <- don[["TMP"]][, idx]
		}else{
			nom.var <- names(don)
			nom.var <- nom.var[nom.var%in%c("TT", "TT2")]
			if(length(nom.var) == 0) return(NULL)
			if(length(nom.var) == 2) nom.var <- "TT"
			daty <- if(str_trim(plt_pars$Tstep) == "1day") don$date.VAR else don$date
			vals <- don[[nom.var]]
			idx <- switch(str_trim(plt_pars$var), "TN" = 1, "TM" = 2, "TX" = 3)
			vals <- vals[, idx]
		}
	}

	daty <- strptime(daty, format.date, tz = "Africa/Kigali")
	daty <- format(daty, format.limit)
	vals <- vals[daty == str_trim(plt_pars$date)]
	if(length(vals) == 0) vals <- NA
	return(vals)
})

inull <- sapply(AWS.DAT, is.null)
AWS.DAT <- AWS.DAT[!inull]
if(length(AWS.DAT) == 0) stop("No data available\n")
AWS.DAT <- do.call(c, AWS.DAT)
AWS.CRD <- AWS.CRD[!inull, ]
if(!any(!is.na(AWS.DAT))) stop("No data available\n")
AWS.DAT <- cbind(AWS.CRD, values = AWS.DAT)

