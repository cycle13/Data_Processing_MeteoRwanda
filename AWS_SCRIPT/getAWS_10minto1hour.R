

computeAWS.10minto1hr <- function(aws, AWS, OUTDIR){
	fileinfo <- file.path(OUTDIR, paste0(AWS, "_AWS"), "compressed_data", "infos", "AWS", paste0(aws, ".rds"))
	if(!file.exists(fileinfo)) return(NULL)
	info <- readRDS(fileinfo)
	if(is.null(info$hour)){
		scalc <- strptime(info$start, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
	}else{
		sadd <- if(info$hour$full) 4200 else 600
		scalc <- strptime(info$hour$end, "%Y%m%d%H", tz = "Africa/Kigali") + sadd
	}

	filerds <- file.path(OUTDIR, paste0(AWS, "_AWS"), "compressed_data", "data_10minQC", paste0(aws, ".rds"))
	data.aws <- readRDS(filerds)

	icalc <- strptime(data.aws$date, "%Y%m%d%H%M%S", tz = "Africa/Kigali") >= scalc
	if(!any(icalc)) return(NULL)

	data.aws$date <- data.aws$date[icalc]
	if(AWS == "LSI"){
		for(ii in names(data.aws$data))
			data.aws$data[[ii]] <- data.aws$data[[ii]][icalc, , drop = FALSE]
	}
	if(AWS == "REMA"){
		for(ii in names(data.aws$data)){
			if(ii == "WIND") data.aws$data[[ii]] <- data.aws$data[[ii]][icalc, , drop = FALSE]
			else data.aws$data[[ii]] <- data.aws$data[[ii]][icalc]
		}
	}

	data1hr <- getAWS.VARSData.10minto1hr(data.aws, AWS)
	rm(data.aws)
	file1hrs <- file.path(OUTDIR, paste0(AWS, "_AWS"), "compressed_data", "data_1hr", paste0(aws, ".rds"))
	if(file.exists(file1hrs)){
		data.aws <- readRDS(file1hrs)
		for(ii in names(data1hr$data)){
			fooc <- if(ii == "date") c else rbind
			tmp <- if(info$hour$full) data.aws[[ii]] else head(data.aws[[ii]], n = -1)
			data.aws[[ii]] <- fooc(tmp, data1hr$data[[ii]])
		}
	}else data.aws <- data1hr$data

	condon <- gzfile(file1hrs, compression = 9)
	open(condon, "wb")
	saveRDS(data.aws, condon)
	close(condon)

	info$hour <- data1hr$last
	saveRDS(info, file = fileinfo)
	return(0)
}

getAWS.VARSData.10minto1hr <- function(data10min, AWS = "LSI"){
	dates <- strptime(data10min$date, "%Y%m%d%H%M%S", tz = "Africa/Kigali")
	index <- getAWS.index.10minto1hr(dates)
	nl <- length(index)
	full.hour <- if(length(index[[nl]]) < 6) FALSE else TRUE

	outdata <- list()
	outdata[["date"]] <- getAWS.date.10minto1hr(dates)
	for(ii in names(data10min$data)){
		don10min <- data10min$data[[ii]]
		if(ii == "WIND") outdata[[ii]] <- getAWSWind.data.10minto1hr(don10min, index, AWS, 3)
		if(ii == "RR") outdata[[ii]] <- getAWSPrecip.data.10minto1hr(don10min, index, AWS, 5)
		if(ii%in%c("RH", "TT", "TT2", "TT10", "TTg", "TTs", "PRES", "RAD"))
			outdata[[ii]] <- getAWSClimVars.data.10minto1hr(don10min, index, AWS, 4)
	}
	return(list(data = outdata, last = list(end = outdata[["date"]][nl], full = full.hour)))
}

getAWS.date.10minto1hr <- function(dates){
	daty <- format(dates, "%Y%m%d%H%M")
	indx <- split(seq_along(daty), substr(daty, 1, 10))
	temps <- substr(daty[sapply(indx, '[[', 1)], 1, 10)
	return(temps)
}

# getAWS.index.10minto1hr <- function(dates){
# 	daty <- format(dates, "%Y%m%d%H%M")
# 	indx0 <- tapply(dates, substr(daty, 1, 10), function(x) format(x + 600, "%Y%m%d%H%M"))
# 	indx <- relist(match(unlist(indx0), daty), indx0)
# 	return(indx)
# }
getAWS.index.10minto1hr <- function(dates){
	daty <- format(dates, "%Y%m%d%H%M")
	indx0 <- lapply(split(dates, substr(daty, 1, 10)), function(x){
		xx <- seq(strptime(format(x[1], "%Y%m%d%H"), "%Y%m%d%H", tz = "Africa/Kigali")+600, length.out = 6, by = "10 min")
		format(xx, "%Y%m%d%H%M")
	})
	indx <- relist(match(unlist(indx0), daty), indx0)
	indx <- lapply(indx, function(x) x[!is.na(x)])
	return(indx)
}

################################################################################################

getAWSPrecip.data.10minto1hr <- function(don10min, index, AWS, nb.obs.min.precip = 5)
{
	ix <- unlist(index)
	naHr0 <- sapply(index, function(x) length(x[!is.na(x)])) < nb.obs.min.precip

	if(AWS == "LSI") rr <- relist(don10min$Tot[ix], index)
	else if(AWS == "REMA") rr <- relist(don10min[ix], index)
	else return(NULL)

	naHr1 <- sapply(rr, function(x) length(x[!is.na(x)])) < nb.obs.min.precip

	rr <- sapply(rr, sum, na.rm = TRUE)
	rr[naHr0 | naHr1] <- NA
	rr <- matrix(rr, ncol = 1)
	rr <- round(rr, 1)
	return(rr)
}

getAWSWind.data.10minto1hr <- function(don10min, index, AWS, nb.obs.min.var = 4)
{
	ix <- unlist(index)
	naHr0 <- sapply(index, function(x) length(x[!is.na(x)])) < nb.obs.min.var

	if(AWS == "LSI"){
		ff.max <- relist(don10min$Max[ix], index)
		ff.ave <- relist(don10min$Ave[ix], index)
		ff <- don10min$RisVel[ix]
		dd <- don10min$RisDir[ix]
	}else if(AWS == "REMA"){
		ff.max <- relist(don10min$FFmax[ix], index)
		ff <- don10min$FF[ix]
		dd <- don10min$DD[ix]
		ff.ave <- relist(ff, index)
	}else return(NULL)

	naHrAve <- sapply(ff.ave, function(x) length(x[!is.na(x)])) < nb.obs.min.var
	naHrMax <- sapply(ff.max, function(x) length(x[!is.na(x)])) < nb.obs.min.var

	ff.ave <- suppressWarnings(sapply(ff.ave, mean, na.rm = TRUE))
	ff.max <- suppressWarnings(sapply(ff.max, max, na.rm = TRUE))

	xu <- -ff * sin(2*pi*dd/360)
	xv <- -ff * cos(2*pi*dd/360)
	ina <- is.na(xu) | is.na(xv)
	xu[ina] <- NA
	xv[ina] <- NA
	xu <- relist(xu, index)
	xv <- relist(xv, index)
	naHrUV <- sapply(xu, function(x) length(x[!is.na(x)])) < nb.obs.min.var

	mu <- sapply(xu, mean, na.rm = TRUE)
	mv <- sapply(xv, mean, na.rm = TRUE)
	ff.moy <- sqrt(mu^2 + mv^2)
	dd.ave <- (atan2(mu, mv) * 360/2/pi) + ifelse(ff.moy < 1e-14, 0, 180)

	ff.ave[naHr0 | naHrAve] <- NA
	ff.max[naHr0 | naHrMax] <- NA
	ff.moy[naHr0 | naHrUV] <- NA
	dd.ave[naHr0 | naHrUV] <- NA

	wnd <- cbind(ff.ave, ff.moy, dd.ave, ff.max)
	wnd <- round(wnd, 1)

	dimnames(wnd) <- NULL
	return(wnd)
}

getAWSClimVars.data.10minto1hr <- function(don10min, index, AWS, nb.obs.min.var = 4)
{
	ix <- unlist(index)
	naHr0 <- sapply(index, function(x) length(x[!is.na(x)])) < nb.obs.min.var

	if(AWS == "LSI"){
		tmp.min <- relist(don10min$Min[ix], index)
		tmp.ave <- relist(don10min$Ave[ix], index)
		tmp.max <- relist(don10min$Max[ix], index)
	}else if(AWS == "REMA"){
		tmp.min <- relist(don10min[ix], index)
		tmp.ave <- tmp.min
		tmp.max <- tmp.min
	}else return(NULL)

	naHrMin <- sapply(tmp.min, function(x) length(x[!is.na(x)])) < nb.obs.min.var
	naHrAve <- sapply(tmp.ave, function(x) length(x[!is.na(x)])) < nb.obs.min.var
	naHrMax <- sapply(tmp.max, function(x) length(x[!is.na(x)])) < nb.obs.min.var

	tmp.min <- suppressWarnings(sapply(tmp.min, min, na.rm = TRUE))
	tmp.ave <- suppressWarnings(sapply(tmp.ave, mean, na.rm = TRUE))
	tmp.max <- suppressWarnings(sapply(tmp.max, max, na.rm = TRUE))

	tmp.min[naHr0 | naHrMin] <- NA
	tmp.ave[naHr0 | naHrAve] <- NA
	tmp.max[naHr0 | naHrMax] <- NA

	tmp <- cbind(tmp.min, tmp.ave, tmp.max)
	tmp <- round(tmp, 1)
	dimnames(tmp) <- NULL

	return(tmp)
}
