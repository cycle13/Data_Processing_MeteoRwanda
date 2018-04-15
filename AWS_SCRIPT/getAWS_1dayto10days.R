
addDekads <- function(daty, n = 1){
	idek <- as.numeric(substr(format(daty,'%Y%m%d'), 8, 8))+n
	dek <- idek%%3
	if(dek == 0) dek <- 3
	daty <- format(addMonths(daty, floor((idek-1)/3)), '%Y-%m')
	daty <- as.Date(paste(daty, dek, sep = '-'))
	return(daty)
}

addMonths <- function(daty, n = 1){
	date0 <- seq(daty, by = paste(n, "months"), length = 2)[2]
	date1 <- seq(as.Date(paste(format(daty,'%Y-%m'),'01', sep = '-')), by = paste(n+1, "months"), length = 2)[2]-1
	daty <- if(date0 > date1) date1 else date0
	return(daty)
}

computeAWS.1dayto10days <- function(aws, AWS, OUTDIR){
	fileinfo <- file.path(OUTDIR, paste0(AWS, "_AWS"), "compressed_data", "infos", "AWS", paste0(aws, ".rds"))
	if(!file.exists(fileinfo)) return(NULL)
	info <- readRDS(fileinfo)

	if(is.null(info$dekad)){
		scalc <- format(strptime(info$start, "%Y%m%d%H%M%S", tz = "Africa/Kigali"), "%Y%m%d")
		scalc <- strptime(scalc, "%Y%m%d", tz = "Africa/Kigali")
	}else{
		if(!is.null(info$dekad$end.temp)){
			dekdaty <- as.Date(info$dekad$end.temp, "%Y%m%d")
			scalc0 <- if(info$dekad$full.temp) as.Date(addDekads(dekdaty)) else dekdaty
		}
		if(!is.null(info$dekad$end.rr)){
			dekdaty <- as.Date(info$dekad$end.rr, "%Y%m%d")
			scalc1 <- if(info$dekad$full.rr) as.Date(addDekads(dekdaty)) else dekdaty
		}

		if(!is.null(info$dekad$end.temp) & !is.null(info$dekad$end.rr)) scalc <- min(scalc0, scalc1)
		else if(!is.null(info$dekad$end.temp) & is.null(info$dekad$end.rr)) scalc <- scalc0
		else if(is.null(info$dekad$end.temp) & !is.null(info$dekad$end.rr)) scalc <- scalc1
		else return(NULL)
		scalc <- paste0(format(scalc, "%Y%m"), c(1, 11, 21)[as.numeric(format(scalc, '%d'))])
		scalc <- strptime(scalc, "%Y%m%d", tz = "Africa/Kigali")
	}

	file1dy <- file.path(OUTDIR, paste0(AWS, "_AWS"), "compressed_data", "data_daily", paste0(aws, ".rds"))
	data.aws <- readRDS(file1dy)

	icalc.RR <- strptime(data.aws$date.RR, "%Y%m%d", tz = "Africa/Kigali") >= scalc
	if(any(icalc.RR)){
		data.aws[["date.RR"]] <- data.aws[["date.RR"]][icalc.RR]
		data.aws[["RR"]] <- data.aws[["RR"]][icalc.RR, , drop = FALSE]
		out.rr <- getAWSPrecip.data.1dayto10days(data.aws, 8)
	}else{
		if(is.null(data.aws$date.VAR)) return(NULL)
	}

	if(!is.null(data.aws$date.VAR)){
		icalc.VAR <- strptime(data.aws$date.VAR, "%Y%m%d", tz = "Africa/Kigali") >= scalc
		if(any(icalc.VAR)){
			nom.var <- names(data.aws)
			nom.var <- nom.var[nom.var%in%c("TT", "TT2")]
			if(length(nom.var) > 0){
				data.aws[["date.VAR"]] <- data.aws[["date.VAR"]][icalc.VAR]
				if(length(nom.var) == 2) nom.var <- "TT"
				data.aws[["temp"]] <- data.aws[[nom.var]][icalc.VAR, , drop = FALSE]
				out.tmp <- getAWSTemp.data.1dayto10days(data.aws, 5)
			}else out.tmp <- NULL
		}else return(NULL)
	}else out.tmp <- NULL

	filedek <- file.path(OUTDIR, paste0(AWS, "_AWS"), "compressed_data", "data_dekad", paste0(aws, ".rds"))
	if(file.exists(filedek)){
		data.aws <- readRDS(filedek)
		if(!is.null(out.rr$last$end.rr)){
			for(ii in c("date.RR", "RR")){
				tmp0 <- if(info$dekad$full.rr) data.aws[[ii]] else head(data.aws[[ii]], n = -1)
				## test last date for data.aws and first date for out.rr
				if(ii == "date.RR"){
					data.aws[[ii]] <- c(tmp0, out.rr$date)
				}else{
					data.aws[[ii]] <- rbind(tmp0, out.rr$data)
				}
			}
			info$dekad$end.rr <- out.rr$last$end.rr
			info$dekad$full.rr <- out.rr$last$full.rr
		}

		if(!is.null(out.tmp$last$end.temp)){
			for(ii in c("date.TMP", "TMP")){
				tmp0 <- if(info$dekad$full.temp) data.aws[[ii]] else head(data.aws[[ii]], n = -1)
				## test last date for data.aws and first date for out.tmp
				if(ii == "date.TMP"){
					data.aws[[ii]] <- c(tmp0, out.tmp$date)
				}else{
					data.aws[[ii]] <- rbind(tmp0, out.tmp$data)
				}
			}
			info$dekad$end.temp <- out.tmp$last$end.temp
			info$dekad$full.temp <- out.tmp$last$full.temp
		}
	}else{
		if(is.null(out.tmp)){
			data.aws <- list(date.RR = out.rr$date, RR = out.rr$data)
			info$dekad <- out.rr$last
		}else{
			data.aws <- list(date.RR = out.rr$date, RR = out.rr$data, date.TMP = out.tmp$date, TMP = out.tmp$data)
			info$dekad <- c(out.rr$last, out.tmp$last)
		}
	}

	condon <- gzfile(filedek, compression = 9)
	open(condon, "wb")
	saveRDS(data.aws, condon)
	close(condon)

	saveRDS(info, file = fileinfo)

	rm(data.aws, out.rr, out.tmp)
	return(0)
}

getAWS.index.1dayto10days <- function(dates){
	dek <- as.numeric(substr(dates, 7, 8))
	dek <- findInterval(dek, c(1, 10, 20, 31), rightmost.closed = TRUE, left.open = TRUE)
	yymodk <- paste0(substr(dates, 1, 6), dek)
	indx <- split(seq_along(dates), yymodk)
	return(indx)
}

getAWSPrecip.data.1dayto10days <- function(don1day, nb.obs.min.precip = 8)
{
	index <- getAWS.index.1dayto10days(don1day[["date.RR"]])
	ix <- unlist(index)
	naDay0 <- sapply(index, length) < nb.obs.min.precip
	rr <- relist(don1day[["RR"]][ix, 1], index)
	naDay1 <- sapply(rr, function(x) length(x[!is.na(x)])) < nb.obs.min.precip
	rr <- sapply(rr, sum, na.rm = TRUE)
	rr[naDay0 | naDay1] <- NA
	rr <- matrix(rr, ncol = 1)
	rr <- round(rr, 1)

	daty <- names(index)
	dekdt <- daty[length(daty)]
	if(substr(dekdt, 7, 7) == "3"){
		if(is.leapyear(as.numeric(substr(dekdt, 1, 4))) && substr(dekdt, 5, 6) == "02") lendek <- 9
		else lendek <- c(11, 8, 11, 10, 11, 10, 11, 11, 10, 11, 10, 11)[as.numeric(substr(dekdt, 5, 6))]
	}else lendek <- 10
	full.day <- if(length(index[[length(index)]]) < lendek) FALSE else TRUE
	rr <- list(date = daty, data = rr, last = list(end.rr = dekdt, full.rr = full.day))
	return(rr)
}

getAWSTemp.data.1dayto10days <- function(don1day, nb.obs.min.var = 5)
{
	index <- getAWS.index.1dayto10days(don1day[["date.VAR"]])
	ix <- unlist(index)
	naDay0 <- sapply(index, length) < nb.obs.min.var

	tmp.min <- relist(don1day[["temp"]][ix, 1], index)
	tmp.ave <- relist(don1day[["temp"]][ix, 2], index)
	tmp.max <- relist(don1day[["temp"]][ix, 3], index)

	naDayMin <- sapply(tmp.min, function(x) length(x[!is.na(x)])) < nb.obs.min.var
	naDayAve <- sapply(tmp.ave, function(x) length(x[!is.na(x)])) < nb.obs.min.var
	naDayMax <- sapply(tmp.max, function(x) length(x[!is.na(x)])) < nb.obs.min.var

	tmp.min <- suppressWarnings(sapply(tmp.min, min, na.rm = TRUE))
	tmp.ave <- suppressWarnings(sapply(tmp.ave, mean, na.rm = TRUE))
	tmp.max <- suppressWarnings(sapply(tmp.max, max, na.rm = TRUE))

	tmp.min[naDay0 | naDayMin] <- NA
	tmp.ave[naDay0 | naDayAve] <- NA
	tmp.max[naDay0 | naDayMax] <- NA

	tmp <- cbind(tmp.min, tmp.ave, tmp.max)
	tmp <- round(tmp, 1)
	dimnames(tmp) <- NULL

	daty <- names(index)
	dekdt <- daty[length(daty)]
	if(substr(dekdt, 7, 7) == "3"){
		if(is.leapyear(as.numeric(substr(dekdt, 1, 4))) && substr(dekdt, 5, 6) == "02") lendek <- 9
		else lendek <- c(11, 8, 11, 10, 11, 10, 11, 11, 10, 11, 10, 11)[as.numeric(substr(dekdt, 5, 6))]
	}else lendek <- 10
	full.day <- if(length(index[[length(index)]]) < lendek) FALSE else TRUE
	tmp <- list(date = daty, data = tmp, last = list(end.temp = dekdt, full.temp = full.day))
	return(tmp)
}
