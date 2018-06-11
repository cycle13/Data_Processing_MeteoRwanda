
getREMAAWS.STN <- function(aws, OUTDIR, ftp){
	cat(paste("Processing :", aws), '\n')
	tmpdir <- file.path(OUTDIR, "tmp")
	if(!dir.exists(tmpdir)) dir.create(tmpdir, showWarnings = FALSE, recursive = TRUE)
	tmpfile <- file.path(tmpdir, paste0("gateway_data_", aws, ".txt"))
	on.exit(unlink(tmpfile))
	file0 <- paste0(ftp$AWS$REMA$ftp, paste0("gateway_data_", aws, ".txt"))
	ret <- try(getFTPData(file0, tmpfile, userpwd = ftp$AWS$REMA$userpwd), silent = TRUE)

	if(inherits(ret, "try-error")){
		cat(paste(aws, ": Unable to get >", paste0("gateway_data_", aws, ".txt"), "< from the FTP server\n"))
		return('no')
	}
	if(ret != 0){
		cat(paste(aws, ": Unable to get >", paste0("gateway_data_", aws, ".txt"), "< from the FTP server\n"))
		return('no')
	}

	don <- try(read.table(tmpfile, colClasses = 'character', stringsAsFactors = FALSE), silent = TRUE)
	if(inherits(don, "try-error")){
		cat(paste(aws, ": Unable to read >", paste0("gateway_data_", aws, ".txt"), "\n"))
		return(NULL)
	}

	return(don)
}

# getREMAAWS.STN <- function(aws, OUTDIR, ftp){
# 	cat(aws, '\n')
# 	tmpdir <- file.path(OUTDIR, "tmp")
# 	if(!dir.exists(tmpdir)) dir.create(tmpdir, showWarnings = FALSE, recursive = TRUE)
# 	tmpfile <- file.path(tmpdir, paste0("gateway_data_", aws, ".txt"))
# 	on.exit(unlink(tmpfile))

# 	fileftp <- paste0(ftp$AWS$REMA$ftp, paste0("gateway_data_", aws, ".txt"))
# 	fileinfo <- file.path(OUTDIR, "REMA_AWS", "compressed_data", "infos", "AWS", paste0(aws, ".rds"))

# 	if(file.exists(fileinfo)){
# 		info <- readRDS(fileinfo)
# 		last <- as.numeric(substr(info$end, 1, 12))

# 		tmp <- readLinesFtpFileTail(fileftp, ftp$AWS$REMA$userpwd, 1, 1024)
# 		buffer.factor <- nchar(tmp)
# 		tmp <- unlist(strsplit(tmp, "\t"))[1]
# 		tmp <- strptime(tmp, "%Y-%m-%d %H:%M", tz = "Africa/Kigali")
# 		tmp <- as.numeric(format(tmp, "%Y%m%d%H%M"))

# 		if(last >= tmp) return(NULL)
# 		wdaty <- seq(strptime(info$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali"), strptime(tmp, "%Y%m%d%H%M", tz = "Africa/Kigali"), by = "10 mins")
# 		ndaty <- length(wdaty[-1])
# 		tmp <- try(readLinesFtpFileTail(fileftp, ftp$AWS$REMA$userpwd, ndaty, buffer.factor), silent = TRUE)
# 		if(inherits(tmp, "try-error")) return(NULL)
# 		cat(tmp, file = tmpfile, sep = "\n")
# 	}else{
# 		ret <- try(getFTPData(fileftp, tmpfile, userpwd = ftp$AWS$REMA$userpwd), silent = TRUE)
# 		if(inherits(ret, "try-error")) return(NULL)
# 		if(ret != 0) return(NULL)
# 	}

# 	don <- try(read.table(tmpfile, colClasses = 'character', stringsAsFactors = FALSE), silent = TRUE)
# 	if(inherits(don, "try-error")) return(NULL)
# 	return(don)
# }

getREMAAWS.10minData <- function(aws, don10min, OUTDIR){
	don10min[don10min == "*"] <- NA
	dates <- strptime(paste(don10min[, 1], don10min[, 2]), "%Y-%m-%d %H:%M", tz = "Africa/Kigali")

	nadates <- !is.na(dates)
	dates <- dates[nadates]
	don10min <- don10min[nadates, , drop = FALSE]

	validdates <- dates <= strptime(format(Sys.time(), "%Y%m%d%H%M%S"), "%Y%m%d%H%M%S", tz = "Africa/Kigali")
	dates <- dates[validdates]
	don10min <- don10min[validdates, , drop = FALSE]

	xhead <- as.character(don10min[1, ])
	Weather <- grep("17-", xhead)
	if(length(Weather) == 0) return(NULL)

	####
	REMA_DIR <- file.path(OUTDIR, "REMA_AWS", "compressed_data", "infos", "AWS")
	if(!dir.exists(REMA_DIR)) dir.create(REMA_DIR, showWarnings = FALSE, recursive = TRUE)
	fileinfo <- file.path(REMA_DIR, paste0(aws, ".rds"))
	if(file.exists(fileinfo)){
		info <- readRDS(fileinfo)
		idaty <- dates > strptime(info$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
		if(!any(idaty)) return("no.update")

		dates <- dates[idaty]
		don10min <- don10min[idaty, , drop = FALSE]
	}

	outdata <- list()
	if(any(grepl("17-11", xhead))) outdata[["RR"]] <- as.numeric(don10min[, which(xhead == "17-11")+1])
	if(any(grepl("17-10", xhead))) outdata[["RH"]] <- as.numeric(don10min[, which(xhead == "17-10")+1])
	if(any(grepl("17-9", xhead))) outdata[["TT"]] <- as.numeric(don10min[, which(xhead == "17-9")+1])
	if(any(grepl("17-1", xhead))) outdata[["TTs"]] <- as.numeric(don10min[, which(xhead == "17-1")+1])
	if(any(grepl("17-2", xhead))) outdata[["PRES"]] <- as.numeric(don10min[, which(xhead == "17-2")+1])
	if(any(grepl("17-7", xhead))) outdata[["RAD"]] <- as.numeric(don10min[, which(xhead == "17-7")+1])
	if(any(c("17-3", "17-8", "17-14")%in%xhead)){
		FF <- if(any(grepl("17-8", xhead))) as.numeric(don10min[, which(xhead == "17-8")+1]) else NA
		DD <- if(any(grepl("17-3", xhead))) as.numeric(don10min[, which(xhead == "17-3")+1]) else NA
		FFmax <- if(any(grepl("17-14", xhead))) as.numeric(don10min[, which(xhead == "17-14")+1]) else NA
		outdata[["WIND"]] <- data.frame(FF = FF, DD = DD, FFmax = FFmax)
	}

	daty <- format(dates, "%Y%m%d%H%M%S")
	if(!file.exists(fileinfo)){
		info <- list(start = daty[1], end = daty[length(daty)])
		info$vars <- names(outdata)
	}else{
		info$end <- daty[length(daty)]
		# ## test if new variables are added or removed???
		# if(!isTRUE(all.equal(info$vars, names(outdata)))){
		# 	## added
		# 	if(length(names(outdata)) > length(info$vars)){
		# 		newvars <- !(names(outdata)%in%info$vars)

		# 	}
		# 	## removed
		# 	if(length(names(outdata)) < length(info$vars)){

		# 	}
		# }
	}
	saveRDS(info, file = fileinfo)

	return(list(date = daty, data = outdata))
}

getREMAAWS.SimpleQC <- function(data10min){
	outdata <- list()
	dates <- data10min$date
	for(ii in names(data10min$data)){
		don10m <- data10min$data[[ii]]
		if(ii == "WIND"){
			# Plausible value check 
			don10m[with(don10m, !is.na(FF) & (FF < 0 | FF > 50)), "FF"] <- NA
			## Gust km/h???
			don10m[with(don10m, !is.na(FFmax) & (FFmax < 0 | FFmax > 180)), "FFmax"] <- NA
			don10m[with(don10m, !is.na(DD) & (DD < 0 | DD > 360)), "DD"] <- NA
		}else{
			# Plausible value check
			if(ii == "RR"){
				xmin <- 0
				xmax <- 50
			}
			if(ii == "TT"){
				xmin <- -5
				xmax <- 50
			}
			if(ii == "TTs"){
				xmin <- -10
				xmax <- 60
			}
			if(ii == "RH"){
				xmin <- 1
				xmax <- 100
			}
			if(ii == "PRES"){
				xmin <- 600
				xmax <- 1040
			}
			if(ii == "RAD"){
				xmin <- 0
				xmax <- 1600
			}
			don10m[!is.na(don10m) & (don10m < xmin | don10m > xmax)] <- NA
		}
		outdata[[ii]] <- don10m
	}
	return(list(date = dates, data = outdata))
}

getREMAAWS.write10minData <- function(aws, OUTDIR, ftpserver){
	data10min <- getREMAAWS.STN(aws, OUTDIR, ftpserver)
	if(is.null(data10min)){
		return("abort")
	}else{
		if(!is.data.frame(data10min))
			if(data10min == 'no') return("try.again")
	}
	data10min <- getREMAAWS.10minData(aws, data10min, OUTDIR)
	if(is.null(data10min)) return("try.again")
	if(!is.list(data10min))
		if(data10min == "no.update") return("no.update")

	REMA_DIR <- file.path(OUTDIR, "REMA_AWS", "compressed_data", "data_10min")
	if(!dir.exists(REMA_DIR)) dir.create(REMA_DIR, showWarnings = FALSE, recursive = TRUE)
	file10min <- file.path(REMA_DIR, paste0(aws, ".rds"))
	if(file.exists(file10min)){
		data.aws <- readRDS(file10min)
		data.aws$date <- c(data.aws$date, data10min$date)
		## added or removed variables???
		for(ii in names(data10min$data)){
			fooc <- if(ii == "WIND") rbind else c
			data.aws$data[[ii]] <- fooc(data.aws$data[[ii]], data10min$data[[ii]])
		}
	}else data.aws <- data10min
	con10min <- gzfile(file10min, compression = 9)
	open(con10min, "wb")
	saveRDS(data.aws, con10min)
	close(con10min)
	rm(data.aws)

	data10minqc <- getREMAAWS.SimpleQC(data10min)
	REMA_DIR <- file.path(OUTDIR, "REMA_AWS", "compressed_data", "data_10minQC")
	if(!dir.exists(REMA_DIR)) dir.create(REMA_DIR, showWarnings = FALSE, recursive = TRUE)
	file10minqc <- file.path(REMA_DIR, paste0(aws, ".rds"))
	if(file.exists(file10minqc)){
		data.aws <- readRDS(file10minqc)
		data.aws$date <- c(data.aws$date, data10minqc$date)
		## added or removed variables???
		for(ii in names(data10minqc$data)){
			fooc <- if(ii == "WIND") rbind else c
			data.aws$data[[ii]] <- fooc(data.aws$data[[ii]], data10minqc$data[[ii]])
		}
	}else data.aws <- data10minqc
	con10minqc <- gzfile(file10minqc, compression = 9)
	open(con10minqc, "wb")
	saveRDS(data.aws, con10minqc)
	close(con10minqc)
	rm(data.aws, data10minqc, data10min)
	return("OK")
}

