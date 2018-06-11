
getLSIAWS.STN <- function(aws, OUTDIR, ftp){
	cat(paste("Processing :", aws), '\n')
	tmpdir <- file.path(OUTDIR, "tmp")
	if(!dir.exists(tmpdir)) dir.create(tmpdir, showWarnings = FALSE, recursive = TRUE)
	curl <- getCurlHandle(userpwd = ftp$AWS$LSI$userpwd, ftp.use.epsv = FALSE, dirlistonly = TRUE)
	listFile <- try(getURL(paste0(ftp$AWS$LSI$ftp, aws, "/"), curl = curl), silent = TRUE)
	rm(curl); gc()
	if(inherits(listFile, "try-error")){
		cat("Unable to connect to server\n")
		return(NULL)
	}

	listFile <- unlist(strsplit(listFile, "\r?\n"))

	iold <- grep("Data[[:digit:]]+.Elab.txt", listFile)
	inew0 <- grep("Data_%FN%_[[:digit:]]+.Elab.txt", listFile)
	inew1 <- grep("Data_[[:digit:]]+__[[:digit:]]+.Elab.txt", listFile)
	inew2 <- grep("Data_[[:digit:]]+_[[:digit:]]+_[[:digit:]]+.Elab.txt", listFile)
	inew3 <- grep("Data_[[:digit:]]+_[[:digit:]]+.Elab.txt", listFile)

	listF1 <- listFile[iold]
	listF2 <- listFile[inew0]
	listF3 <- listFile[inew1]
	listF4 <- listFile[inew2]
	listF5 <- listFile[inew3]
	if(length(listF1) == 0 & length(listF2) == 0 &
		length(listF3) == 0 & length(listF4) == 0 & length(listF5) == 0)
	{
		cat(paste(aws, ": No valid filename format found\n"))
		return("NoValid")
	}

	wdaty1 <- if(length(listF1) == 0) NULL else
		strptime(substr(listF1, 5, 18), "%Y%m%d%H%M%S", tz = "Africa/Kigali")
	wdaty2 <- if(length(listF2) == 0) NULL else
		strptime(paste0(substr(listF2, 11, 18), "_000001"), "%Y%m%d_%H%M%S", tz = "Africa/Kigali")
	wdaty3 <- if(length(listF3) == 0) NULL else
		strptime(paste0(substr(listF3, 16, 23), "_000001"), "%Y%m%d_%H%M%S", tz = "Africa/Kigali")
	wdaty4 <- if(length(listF4) == 0) NULL else
		strptime(substr(listF4, 15, 29), "%Y%m%d_%H%M%S", tz = "Africa/Kigali")
	wdaty5 <- if(length(listF5) == 0) NULL else
		strptime(paste0(substr(listF5, 15, 22), "_000001"), "%Y%m%d_%H%M%S", tz = "Africa/Kigali")

	wdaty <- list(wdaty1, wdaty2, wdaty3, wdaty4, wdaty5)
	inull <- sapply(wdaty, is.null)
	wdaty <- wdaty[!inull]
	wdaty <- if(length(wdaty) > 1) do.call(c, wdaty) else wdaty[[1]]
	listFile <- c(listF1, listF2, listF3, listF4, listF5)
	ina <- !is.na(wdaty)
	wdaty <- wdaty[ina]
	listFile <- listFile[ina]
	if(length(listFile) == 0)
	{
		cat(paste(aws, ": No valid filename format found\n"))
		return("NoValid")
	}

	oo <- order(wdaty)
	wdaty <- wdaty[oo]
	listFile <- listFile[oo]

	fileinfo <- file.path(OUTDIR, "LSI_AWS", "compressed_data", "infos", "AWS", paste0(aws, ".rds"))
	if(file.exists(fileinfo)){
		info <- readRDS(fileinfo)
		last <- strptime(info$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
		ilast <- which(wdaty >= last)
		if(length(ilast) == 0){
			cat(paste(aws, ": Data up to date\n"))
			return("Uptodate")
		}
		ilast <- if(ilast[1] == 1) ilast else c(ilast[1]-1, ilast)
		listFile <- listFile[ilast]
	}

	donne <- lapply(listFile, function(ff){
		cat(paste(aws, ": Downloading >", ff), '\n')
		tmpfile <- file.path(tmpdir, ff)
		on.exit(unlink(tmpfile))
		file0 <- paste0(ftp$AWS$LSI$ftp, aws, "/", ff)
		ret <- try(getFTPData(file0, tmpfile, userpwd = ftp$AWS$LSI$userpwd), silent = TRUE)

		if(inherits(ret, "try-error")){
			cat(paste(aws, ": Unable to get >", ff, "< from the FTP server\n"))
			return("no")
		}
		if(ret != 0){
			cat(paste(aws, ": Unable to get >", ff, "< from the FTP server\n"))
			return("no")
		}
		don <- try(readLines(tmpfile), silent = TRUE)
		if(inherits(don, "try-error")){
			cat(paste(aws, ": Unable to read >", ff, "\n"))
			return(NULL)
		}

		xhead <- don[1:2]
		h1 <- strsplit(xhead[1], "\t")
		h2 <- strsplit(xhead[2], "\t")
		xhead <- c(h1, h2)
		don <- don[-(1:3)]
		don <- str_trim(don)
		don <- don[don != ""]
		if(length(don) == 0){
			cat(paste(aws, ": No data inside >", ff, "\n"))
			return(NULL)
		}
		don <- try(do.call(rbind, lapply(seq_along(don), function(i) unlist(strsplit(don[i], "\t")))), silent = TRUE)
		if(inherits(don, "try-error")){
			cat(paste(aws, ": Unable to parse data >", ff, "\n"))
			return(NULL)
		}
		if(ncol(don) == 1){ 
			cat(paste(aws, ": Unknown data delimiters >", ff), "\n")
			return(NULL)
		}

		daty1 <- strptime(don[, 1], "%d/%m/%Y %I:%M:%S %p", tz = "Africa/Kigali")
		daty2 <- strptime(don[, 1], "%d/%m/%Y %H.%M.%S", tz = "Africa/Kigali")
		if(all(is.na(daty1)) & all(is.na(daty2))){
			cat(paste(aws, ": Unknown date format >", ff), "\n")
			return(NULL)
		}
		daty <- if(all(is.na(daty1))) daty2 else daty1
		don[, 1] <- format(daty, "%d/%m/%Y %I:%M:%S %p", tz = "Africa/Kigali")

		list(head = xhead, data = don, date = daty)
	})

	inull <- sapply(donne, is.null)
	donne <- donne[!inull]
	if(length(donne) == 0){
		cat(paste(aws, ": All files are considered invalid\n"))
		return("NoValid")
	}

	ino <- sapply(donne, function(x) if(!is.list(x)) x == "no" else FALSE)
	if(any(ino)){
		cat(paste(aws, ": Unable to download some files\n"))
		return(NULL)
	}

	xhead <- donne[[1]]$head
	daty <- do.call(c, lapply(donne, '[[', 'date'))
	donne <- do.call(rbind, lapply(donne, '[[', 'data'))
	ix <- order(daty)
	daty <- daty[ix]
	donne <- donne[ix, , drop = FALSE]
	daty <- format(daty, "%d/%m/%Y %I:%M:%S %p", tz = "Africa/Kigali")
	donne <- donne[!duplicated(daty, fromLast =TRUE), , drop = FALSE]
	list(head = xhead, data = donne)
}

getLSIAWS.AllVariables <- function(entete){
	params <- lapply(entete, function(x){
		x1 <- str_trim(x[[1]])
		x1 <- x1[x1 != ""]
		x1 <- x1[-1]
		xx1 <- strsplit(x1, " ")
		vars <- sapply(xx1, function(y){
			y <- str_trim(y)
			if(y[1] == "T-AIR") paste0(y[1], " ", y[2]) else y[1]
		})

		rr.v <- which(vars%in%c("RAIN", "Prec"))
		RR <- if(length(rr.v) > 0) rr.v else NA
		rh.v <- which(vars%in%c("RELHumidity", "Humidity"))
		RH <- if(length(rh.v) > 0) rh.v else NA
		airtmp.v <- which(vars%in%c("Temperatur", "TempArea", "Temperature", "AIRTemp"))
		tempair <- if(length(airtmp.v) > 0) airtmp.v else NA
		airt2.v <- which(vars%in%c("T-AIR 2", "T-AIR 2M", "T-AIR 2m"))
		temp2m <- if(length(airt2.v) > 0) airt2.v else NA
		airt10.v <- which(vars%in%c("T-AIR 10", "T-AIR 10M", "T-AIR 10m"))
		temp10m <- if(length(airt10.v) > 0) airt10.v else NA
		tsoil.v <- which(vars%in%"T-SOIL")
		tsoil <- if(length(tsoil.v) > 0) tsoil.v else NA
		tgrnd.v <- which(vars%in%"T-GROUND")
		tgrnd <- if(length(tgrnd.v) > 0) tgrnd.v else NA
		press.v <- which(vars%in%c("ATMPressure", "PressATM", "Atm.press"))
		press <- if(length(press.v) > 0) press.v else NA
		radgbl.v <- which(vars%in%c("Glob.Rad.", "RadGLOBal", "Glob.Rad", "GLOBALRad"))
		radgbl <- if(length(radgbl.v) > 0) radgbl.v else NA
		ff.v <- which(vars%in%"WindSPEED")
		FF <- if(length(ff.v) > 0) ff.v else NA
		dd.v <- which(vars%in%"WindDIR")
		DD <- if(length(dd.v) > 0) dd.v else NA
		intmp.v <- which(vars%in%c("TempINTerna", "Int.Temp.", "INSideTemp", "Internal"))
		intmp <- if(length(intmp.v) > 0) intmp.v else NA
		power.v <- which(vars%in%c("Power", "POWER", "power", "POWERSupply", "BATTLevel"))
		power <- if(length(power.v) > 0) power.v else NA

		varpos <- list(pars = c("RR", "RH", "TT", "TT2", "TT10", "TTg", "TTs", "PRES", "RAD", "FF", "DD", "TMPi", "POWER"),
			pos = c(RR, RH, tempair, temp2m, temp10m, tgrnd, tsoil, press, radgbl, FF, DD, intmp, power))

		x2 <- str_trim(x[[2]])
		xx2 <- tolower(x2)
		ncol <- length(xx2)
		strt <- which(xx2%in%c("min", "tot", "inst", "prevdir"))
		if(length(strt) == 0) return(NULL)
		end <- c(strt[-1]-1, ncol)
		header <- lapply(seq_along(strt), function(j) x2[strt[j]:end[j]])
		values <- list(ncol = ncol, start = strt, end = end, header = header)
		list(params = varpos, values = values)
	})
	return(params)
}

getLSIAWS.10minData <- function(aws, don10min, OUTDIR){
	params <- getLSIAWS.AllVariables(list(don10min$head))[[1]]
	if(is.null(params)){
		cat(paste(aws, ": It seems that your header is messed up!\n"))
		return(NULL)
	}
	don <- don10min$data
	don[don == "-999990.00" | don == "-999999.00"] <- NA
	dates <- strptime(don[, 1], "%d/%m/%Y %I:%M:%S %p", tz = "Africa/Kigali")

	nadates <- !is.na(dates)
	dates <- dates[nadates]
	don <- don[nadates, , drop = FALSE]

	time.now <- format(Sys.time(), "%Y%m%d%H%M%S", tz = "Africa/Kigali")
	validdates <- dates <= strptime(time.now, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
	dates <- dates[validdates]
	don <- don[validdates, , drop = FALSE]

	####
	LSI_DIR <- file.path(OUTDIR, "LSI_AWS", "compressed_data", "infos", "AWS")
	if(!dir.exists(LSI_DIR)) dir.create(LSI_DIR, showWarnings = FALSE, recursive = TRUE)
	fileinfo <- file.path(LSI_DIR, paste0(aws, ".rds"))
	if(file.exists(fileinfo)){
		info <- readRDS(fileinfo)
		idaty <- dates > strptime(info$end, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
		if(!any(idaty)) return("no.update")

		dates <- dates[idaty]
		don <- don[idaty, , drop = FALSE]
	}

	varexist <- !is.na(params$params$pos)
	varname <- params$params$pars[varexist]
	varpos <- params$params$pos[varexist]

	PARS <- c("RR", "RH", "TT", "TT2", "TT10", "TTg", "TTs", "PRES", "RAD", "WIND")
	outdata <- list()
	for(ii in PARS){
		if(ii == "WIND"){
			iff <- grep("^FF$", varname)
			idd <- grep("^DD$", varname)
			if(length(iff) == 0 | length(idd) == 0 ) next
			FF10min <- don[, params$values$start[varpos[iff]]:params$values$end[varpos[iff]], drop = FALSE]
			DD10min <- don[, params$values$start[varpos[idd]]:params$values$end[varpos[idd]], drop = FALSE]
			data10min <- apply(cbind(FF10min, DD10min), 2, as.numeric)
			data10min <- data.frame(data10min, stringsAsFactors = FALSE)
			nameFF <- params$values$header[[varpos[iff]]]
			nameFF[nameFF == "ValidDataPerc"] <- "ValidDataPerc.FF"
			nameDD <- params$values$header[[varpos[idd]]]
			nameDD[nameDD == "ValidDataPerc"] <- "ValidDataPerc.DD"
			names(data10min) <- c(nameFF, nameDD)
		}else{
			ipar <- grep(paste0("^", ii, "$"), varname)
			if(length(ipar) == 0) next
			donvar <- don[, params$values$start[varpos[ipar]]:params$values$end[varpos[ipar]], drop = FALSE]
			donvar <- apply(donvar, 2, as.numeric)
			data10min <- data.frame(donvar, stringsAsFactors = FALSE)
			names(data10min) <- params$values$header[[varpos[ipar]]]
		}
		outdata[[ii]] <- data10min
	}
	if(length(outdata) == 0) outdata <- NULL

	daty <- format(dates, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
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

getLSIAWS.SimpleQC <- function(data10min, ValidDataPerc.min = 50){
	outdata <- list()
	dates <- data10min$date
	for(ii in names(data10min$data)){
		don10m <- data10min$data[[ii]]
		if(ii == "WIND"){
			ValidDataPerc.FF <- any(grepl("ValidDataPerc.FF", names(don10m), ignore.case = TRUE))
			ValidDataPerc.DD <- any(grepl("ValidDataPerc.DD", names(don10m), ignore.case = TRUE))
			if(ValidDataPerc.FF){
				prFF <- don10m$ValidDataPerc.FF
				don10m[is.na(prFF) | prFF < ValidDataPerc.min, c("Min", "Ave", "Max", "StdDev")] <- NA
			}
			if(ValidDataPerc.DD){
				prDD <- don10m$ValidDataPerc.DD
				don10m[is.na(prDD) | prDD < ValidDataPerc.min, c("PrevDir", "RisDir", "RisVel", "StdDevDir", "CalmPerc")] <- NA
			}

			# Plausible value check 
			don10m[with(don10m, !is.na(Min) & (Min < 0 | Min > 75)), "Min"] <- NA
			don10m[with(don10m, !is.na(Ave) & (Ave < 0 | Ave > 75)), "Ave"] <- NA
			don10m[with(don10m, !is.na(Max) & (Max < 0 | Max > 75)), "Max"] <- NA
			
			don10m[with(don10m, !is.na(PrevDir) & (PrevDir < 0 | PrevDir > 360)), "PrevDir"] <- NA
			don10m[with(don10m, !is.na(RisDir) & (RisDir < 0 | RisDir > 360)), "RisDir"] <- NA
			don10m[with(don10m, !is.na(RisVel) & (RisVel < 0 | RisVel > 75)), "RisVel"] <- NA
			don10m[with(don10m, !is.na(CalmPerc) & (CalmPerc < 0 | CalmPerc > 75)), "CalmPerc"] <- NA
			don10m <- don10m[, c("Min", "Ave", "Max", "PrevDir", "RisDir", "RisVel", "CalmPerc")]
		}else{
			ValidDataPerc <- any(grepl("ValidDataPerc", names(don10m), ignore.case = TRUE))
			if(ValidDataPerc){
				pr <- don10m$ValidDataPerc
				don10m[is.na(pr) | pr < ValidDataPerc.min, ] <- NA
			}

			# Plausible value check
			if(ii == "RR"){
				don10m[with(don10m, !is.na(Tot) & (Tot < 0 | Tot > 50)), "Tot"] <- NA
				don10m <- don10m[, "Tot", drop = FALSE]
			}
			if(ii%in%c("TT", "TT2", "TT10")){
				don10m[with(don10m, !is.na(Min) & (Min < -5 | Min > 50)), "Min"] <- NA
				don10m[with(don10m, !is.na(Ave) & (Ave < -5 | Ave > 50)), "Ave"] <- NA
				don10m[with(don10m, !is.na(Max) & (Max < -5 | Max > 50)), "Max"] <- NA

				# StdDev <- any(grepl("StdDev", names(don10m), ignore.case = TRUE))
				# if(StdDev){
				# 	ix <- with(don10m, abs(Max-Ave)+abs(Ave-Min)) > 5*don10m$StdDev
				# 	don10m[!is.na(ix) & ix, ] <- NA
				# }
				don10m <- don10m[, c("Min", "Ave", "Max"), drop = FALSE]
			}
			if(ii%in%c("TTg", "TTs")){
				don10m[with(don10m, !is.na(Min) & (Min < -10 | Min > 60)), "Min"] <- NA
				don10m[with(don10m, !is.na(Ave) & (Ave < -10 | Ave > 60)), "Ave"] <- NA
				don10m[with(don10m, !is.na(Max) & (Max < -10 | Max > 60)), "Max"] <- NA
				don10m <- don10m[, c("Min", "Ave", "Max"), drop = FALSE]
			}
			if(ii == "RH"){
				don10m[with(don10m, !is.na(Min) & (Min < 1 | Min > 100)), "Min"] <- NA
				don10m[with(don10m, !is.na(Ave) & (Ave < 1 | Ave > 100)), "Ave"] <- NA
				don10m[with(don10m, !is.na(Max) & (Max < 1 | Max > 100)), "Max"] <- NA
				don10m <- don10m[, c("Min", "Ave", "Max"), drop = FALSE]
			}
			if(ii == "PRES"){
				don10m[with(don10m, !is.na(Min) & (Min < 600 | Min > 1040)), "Min"] <- NA
				don10m[with(don10m, !is.na(Ave) & (Ave < 600 | Ave > 1040)), "Ave"] <- NA
				don10m[with(don10m, !is.na(Max) & (Max < 600 | Max > 1040)), "Max"] <- NA
				don10m <- don10m[, c("Min", "Ave", "Max"), drop = FALSE]
			}
			if(ii == "RAD"){
				don10m[with(don10m, !is.na(Min) & (Min < 0 | Min > 1600)), "Min"] <- NA
				don10m[with(don10m, !is.na(Ave) & (Ave < 0 | Ave > 1600)), "Ave"] <- NA
				don10m[with(don10m, !is.na(Max) & (Max < 0 | Max > 1600)), "Max"] <- NA
				don10m <- don10m[, c("Min", "Ave", "Max"), drop = FALSE]
			}
		}
		outdata[[ii]] <- don10m
	}
	return(list(date = dates, data = outdata))
}

getLSIAWS.write10minData <- function(aws, OUTDIR, ftpserver){
	data10min <- getLSIAWS.STN(aws, OUTDIR, ftpserver)
	if(is.null(data10min)){
		return("try.again")
	}else{
		if(!is.list(data10min))
			if(data10min == "NoValid") return("abort")
		if(!is.list(data10min))
			if(data10min == "Uptodate") return("no.update")
	}
	data10min <- getLSIAWS.10minData(aws, data10min, OUTDIR)
	if(is.null(data10min)) return("try.again")
	if(is.list(data10min)){
		if(is.null(data10min$data)) return("abort")
	}else{
		if(data10min == "no.update") return("no.update")
	}

	LSI_DIR <- file.path(OUTDIR, "LSI_AWS", "compressed_data", "data_10min")
	if(!dir.exists(LSI_DIR)) dir.create(LSI_DIR, showWarnings = FALSE, recursive = TRUE)
	file10min <- file.path(LSI_DIR, paste0(aws, ".rds"))
	if(file.exists(file10min)){
		data.aws <- readRDS(file10min)
		data.aws$date <- c(data.aws$date, data10min$date)
		## added or removed variables???
		for(ii in names(data10min$data)){
			data.aws$data[[ii]] <- rbind(data.aws$data[[ii]], data10min$data[[ii]])
		}
	}else data.aws <- data10min
	con10min <- gzfile(file10min, compression = 9)
	open(con10min, "wb")
	saveRDS(data.aws, con10min)
	close(con10min)
	rm(data.aws)

	data10minqc <- getLSIAWS.SimpleQC(data10min, ValidDataPerc.min = 50)
	LSI_DIR <- file.path(OUTDIR, "LSI_AWS", "compressed_data", "data_10minQC")
	if(!dir.exists(LSI_DIR)) dir.create(LSI_DIR, showWarnings = FALSE, recursive = TRUE)
	file10minqc <- file.path(LSI_DIR, paste0(aws, ".rds"))
	if(file.exists(file10minqc)){
		data.aws <- readRDS(file10minqc)
		data.aws$date <- c(data.aws$date, data10minqc$date)
		## added or removed variables???
		for(ii in names(data10minqc$data)){
			data.aws$data[[ii]] <- rbind(data.aws$data[[ii]], data10minqc$data[[ii]])
		}
	}else data.aws <- data10minqc
	con10minqc <- gzfile(file10minqc, compression = 9)
	open(con10minqc, "wb")
	saveRDS(data.aws, con10minqc)
	close(con10minqc)
	rm(data.aws, data10minqc, data10min)
	return("OK")
}


