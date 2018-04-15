
computeAWS.1hrto1day <- function(aws, AWS, OUTDIR){
	fileinfo <- file.path(OUTDIR, paste0(AWS, "_AWS"), "compressed_data", "infos", "AWS", paste0(aws, ".rds"))
	if(!file.exists(fileinfo)) return(NULL)
	info <- readRDS(fileinfo)
	if(is.null(info$day)){
		scalc <- format(strptime(info$start, "%Y%m%d%H%M%S", tz = "Africa/Kigali"), "%Y%m%d%H")
		scalc <- strptime(scalc, "%Y%m%d%H", tz = "Africa/Kigali")
	}else{
		if(!is.null(info$day$end.var)){
			sadd0 <- if(info$day$full.var) 3600*24 else 0
			scalc0 <- strptime(info$day$end.var, "%Y%m%d", tz = "Africa/Kigali") + sadd0
		}
		if(!is.null(info$day$end.rr)){
			sadd1 <- if(info$day$full.rr) 3600*(8+24) else 3600*8
			scalc1 <- strptime(info$day$end.rr, "%Y%m%d", tz = "Africa/Kigali") + sadd1
		}

		if(!is.null(info$day$end.var) & !is.null(info$day$end.rr)) scalc <- min(scalc0, scalc1)
		else if(!is.null(info$day$end.var) & is.null(info$day$end.rr)) scalc <- scalc0
		else if(is.null(info$day$end.var) & !is.null(info$day$end.rr)) scalc <- scalc1
		else return(NULL)
	}

	filerds <- file.path(OUTDIR, paste0(AWS, "_AWS"), "compressed_data", "data_1hr", paste0(aws, ".rds"))
	data.aws <- readRDS(filerds)

	icalc <- strptime(data.aws$date, "%Y%m%d%H", tz = "Africa/Kigali") >= scalc
	if(!any(icalc)) return(NULL)

	for(ii in names(data.aws)){
		if(ii == "date") data.aws[["date"]] <- data.aws[["date"]][icalc]
		else data.aws[[ii]] <- data.aws[[ii]][icalc, , drop = FALSE]
	}

	data1dy <- getAWS.VARSData.1hrto1day(data.aws)
	rm(data.aws)
	file1dy <- file.path(OUTDIR, paste0(AWS, "_AWS"), "compressed_data", "data_daily", paste0(aws, ".rds"))
	if(file.exists(file1dy)){
		data.aws <- readRDS(file1dy)

		if(!is.null(data1dy$last$end.rr)){
			for(ii in c("date.RR", "RR")){
				fooc <- if(ii == "date.RR") c else rbind
				## test last date for data.aws and first date for data1dy
				tmp0 <- if(info$day$full.rr) data.aws[[ii]] else head(data.aws[[ii]], n = -1)
				data.aws[[ii]] <- fooc(tmp0, data1dy$data[[ii]])
			}
		}

		if(!is.null(data1dy$last$end.var)){
			prms <- names(data1dy$data)
			prms <- prms[!prms%in%c("date.RR", "RR")]
			for(ii in prms){
				fooc <- if(ii == "date.VAR") c else rbind
				## test last date for data.aws and first date for data1dy
				tmp1 <- if(info$day$full.var) data.aws[[ii]] else head(data.aws[[ii]], n = -1)
				data.aws[[ii]] <- fooc(tmp1, data1dy$data[[ii]])
			}
		}
	}else data.aws <- data1dy$data

	condon <- gzfile(file1dy, compression = 9)
	open(condon, "wb")
	saveRDS(data.aws, condon)
	close(condon)

	info$day <- data1dy$last
	saveRDS(info, file = fileinfo)
	return(0)
}

getAWS.index.1hrto1day <- function(dates, precip = FALSE){
	daty <- format(dates, "%Y%m%d%H")
	sadd <- if(precip) 3600*8 else 0
	indx0 <- lapply(split(dates, substr(daty, 1, 8)), function(x){
		xx <- seq(strptime(format(x[1], "%Y%m%d"), "%Y%m%d", tz = "Africa/Kigali")+sadd, length.out = 24, by = "hour")
		format(xx, "%Y%m%d%H")
	})
	indx <- relist(match(unlist(indx0), daty), indx0)
	indx <- lapply(indx, function(x) x[!is.na(x)])
	return(indx)
}

getAWS.VARSData.1hrto1day <- function(data1hr){
	dates <- strptime(data1hr$date, "%Y%m%d%H", tz = "Africa/Kigali")

	parms <- names(data1hr)
	parms <- parms[!(parms %in% "date")]

	if("RR"%in%parms){
		index1 <- getAWS.index.1hrto1day(dates, precip = TRUE)
		nl1 <- length(index1)
		full.day1 <- if(length(index1[[nl1]]) < 24) FALSE else TRUE
	}
	if(length(parms) > 1){
		index0 <- getAWS.index.1hrto1day(dates)
		nl0 <- length(index0)
		full.day0 <- if(length(index0[[nl0]]) < 24) FALSE else TRUE
	}

	outdata <- list()
	for(ii in parms){
		don1hr <- data1hr[[ii]]
		if(ii == "RR"){
			outdata[["date.RR"]] <- names(index1)
			outdata[[ii]] <- getAWSPrecip.data.1hrto1day(don1hr, index1, 24)
		}else{
			if(is.null(outdata[["date.VAR"]])) outdata[["date.VAR"]] <- names(index0)
		}
		if(ii%in%c("RH", "TT", "TT2", "TT10", "TTg", "TTs", "PRES", "RAD"))
			outdata[[ii]] <- getAWSClimVars.data.1hrto1day(don1hr, index0, 20)
		if(ii == "WIND") outdata[[ii]] <- getAWSWind.data.1hrto1day(don1hr, index0, 20)
	}
	last.rr <- if("RR"%in%parms) list(end.rr = outdata[["date.RR"]][nl1], full.rr = full.day1) else list(end.rr = NULL, full.rr = NULL)
	last.var <- if(length(parms) > 1) list(end.var = outdata[["date.VAR"]][nl0], full.var = full.day0) else list(end.var = NULL, full.var = NULL)
	return(list(data = outdata, last = c(last.rr, last.var)))
}

getAWSPrecip.data.1hrto1day <- function(don1hr, index, nb.obs.min.precip = 24)
{
	ix <- unlist(index)
	naHr0 <- sapply(index, length) < nb.obs.min.precip

	rr <- relist(don1hr[ix, ], index)

	naHr1 <- sapply(rr, function(x) length(x[!is.na(x)])) < nb.obs.min.precip

	rr <- sapply(rr, sum, na.rm = TRUE)
	rr[naHr0 | naHr1] <- NA
	rr <- matrix(rr, ncol = 1)
	rr <- round(rr, 1)
	return(rr)
}

getAWSWind.data.1hrto1day <- function(don1hr, index, nb.obs.min.var = 20)
{
	ix <- unlist(index)
	naHr0 <- sapply(index, length) < nb.obs.min.var

	ff.max <- relist(don1hr[ix, 4], index)
	ff.ave <- relist(don1hr[ix, 1], index)
	ff <- don1hr[ix, 2]
	dd <- don1hr[ix, 3]

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

getAWSClimVars.data.1hrto1day <- function(don1hr, index, nb.obs.min.var = 20)
{
	ix <- unlist(index)
	naHr0 <- sapply(index, length) < nb.obs.min.var

	tmp.min <- relist(don1hr[ix, 1], index)
	tmp.ave <- relist(don1hr[ix, 2], index)
	tmp.max <- relist(don1hr[ix, 3], index)

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

