
### Path to the folder AWS_DATA
AWS_DATA_DIR <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/AWS_DATA"
### Path to the folder AWS_SCRIPT
AWS_SCRIPT_DIR <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/AWS_SCRIPT"

########################################## End Edit #############################################

readLinesFtpFileTail <- function(url, userpwd, n, buff){
	curl <- getCurlHandle(ftp.use.epsv = FALSE, userpwd = userpwd)
	on.exit({
		rm(curl)
		gc()
	})
	filecon <- getURL(url, nobody = TRUE, header = TRUE, curl = curl)

	filecon <- unlist(strsplit(filecon, "\r\n"))
	size <- gsub("[^[:digit:]]", "", filecon[grep("Content-Length:", filecon)])
	size <- as.numeric(size)

	bufferSize <- as.integer(buff*n)
	pos1 <- size - bufferSize
	pos2 <- size
	text <- character()
	k <- 0L

	while(TRUE){
		chars <- getURL(url, range = paste(pos1, pos2, sep = "-"),
						nobody = FALSE, header = FALSE, curl = curl)

		k <- k + length(gregexpr(pattern = "\\n", text = chars)[[1]])
		text <- paste0(chars, text)

		if(k > n || pos1 == 0) break
		bufferSize <- as.integer(buff*(n-k+10))
		pos2 <- pos1-1
		pos1 <- max(pos1-bufferSize, 0)
		Sys.sleep(2)
	}
	text <- tail(strsplit(text, "\\n")[[1]], n)
	return(text)
}

############################################

getFTPData <- function(file0, tmpfile, userpwd){
	curl <- getCurlHandle(userpwd = userpwd) 
	on.exit({
		rm(curl)
		gc()
	})
	filecon <- getURL(file0, curl = curl)
	don <- readLines(textConnection(filecon))
	cat(don, file = tmpfile, sep = "\n")
	return(0)
}

is.leapyear <- function(year){
	leap <- ((year %% 4 == 0) & (year %% 100 != 0)) | (year %% 400 == 0)
	return(leap)
}

############################################
library(stringr)
library(RCurl)
library(tools)
library(R.utils)

source(file.path(AWS_SCRIPT_DIR, "getLSIAWS_data.R"))
source(file.path(AWS_SCRIPT_DIR, "getREMAAWS_data.R"))
source(file.path(AWS_SCRIPT_DIR, "getAWS_10minto1hour.R"))
source(file.path(AWS_SCRIPT_DIR, "getAWS_1hourto1day.R"))
source(file.path(AWS_SCRIPT_DIR, "getAWS_1dayto10days.R"))
auth <- readRDS(file.path(AWS_DATA_DIR, "coordinates_files", "ftpserver.mto"))

############################################
# 
ret1hr <- lapply(c("REMA", "LSI"), function(AWS){
	cat(paste("Process", AWS, "data ......"), '\n')
	curl <- getCurlHandle(userpwd = auth$AWS[[AWS]]$userpwd, ftp.use.epsv = FALSE, dirlistonly = TRUE)
	listAWS <- try(getURL(auth$AWS[[AWS]]$ftp, curl = curl), silent = TRUE)
	rm(curl); gc()
	if(inherits(listAWS, "try-error")){
		cat(paste("Unable to connect to", AWS), '\n')
		return(NULL)
	}

	listAWS <- unlist(strsplit(listAWS, "\r?\n"))
	if(AWS == "LSI"){
		AWSstn <- listAWS[grep('^[[:digit:]]*$', listAWS)]
		get10minutesData <- getLSIAWS.write10minData
	}

	if(AWS == "REMA"){
		AWSstn <- listAWS[grep("gateway_data_", listAWS)]
		AWSstn <- gsub("[^[:digit:]]", "", AWSstn)
		get10minutesData <- getREMAAWS.write10minData
	}

	### get 10 min
	ret.aws <- lapply(AWSstn, function(aws) get10minutesData(aws, AWS_DATA_DIR, auth))
	awsdown <- unlist(ret.aws)

	redown <- which(awsdown%in%"try.again")
	if(length(redown) > 0){
		ret <- lapply(AWSstn[redown], function(aws) get10minutesData(aws, AWS_DATA_DIR, auth))
		awsdown[redown] <- unlist(ret)
	}

	### get 1 hr
	## Note
	## data at 10 hour are computed using data from 10:10:00 to 11:00:00
	calc1hr <- which(awsdown%in%"OK")
	if(length(calc1hr) == 0) return(NULL)
	ret.1hr <- lapply(AWSstn[calc1hr], function(aws) computeAWS.10minto1hr(aws, AWS, AWS_DATA_DIR))

	awsdown[awsdown == "try.again"] <- "no.update"
	awsdown[awsdown == "OK"] <- "updated"
	res <- lapply(seq_along(AWSstn), function(j){
		fileinfo <- file.path(AWS_DATA_DIR, paste0(AWS, "_AWS"), "compressed_data", "infos", "AWS", paste0(AWSstn[j], ".rds"))
		if(!file.exists(fileinfo)) return(NULL)
		info <- readRDS(fileinfo)
		info$hour$status <- awsdown[j]
		saveRDS(info, file = fileinfo)
	})
	AWSstn <- AWSstn[calc1hr]

	### get 1 day
	## Note 
	## precip for 2014-01-23 are computed using hourly data from 2014-01-23 08:00:00 to 2014-01-24 07:00:00
	## others vars for 2014-01-23 are computed using hourly data from 2014-01-23 00:00:00 to 2014-01-23 23:00:00
	calc1day <- which(!sapply(ret.1hr, is.null))
	if(length(calc1day) == 0) return(NULL)
	ret.1hr <- ret.1hr[calc1day]
	AWSstn <- AWSstn[calc1day]
	calc1day <- which(sapply(ret.1hr, `==`, e2 = 0))
	if(length(calc1day) == 0) return(NULL)
	AWSstn <- AWSstn[calc1day]
	ret.1day <- lapply(AWSstn, function(aws) computeAWS.1hrto1day(aws, AWS, AWS_DATA_DIR))

	### get 10 days
	calc10day <- which(!sapply(ret.1day, is.null))
	if(length(calc10day) == 0) return(NULL)
	ret.1day <- ret.1day[calc10day]
	AWSstn <- AWSstn[calc10day]
	calc10day <- which(sapply(ret.1day, `==`, e2 = 0))
	if(length(calc10day) == 0) return(NULL)
	AWSstn <- AWSstn[calc10day]
	ret.10days <- lapply(AWSstn, function(aws) computeAWS.1dayto10days(aws, AWS, AWS_DATA_DIR))

	cat(paste("Processing", AWS, "data done"), '\n')
	return(0)
})

