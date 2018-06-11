
### Path to the folder RADAR_DATA
radar_data_dir <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/RADAR_DATA"

##############
## Start date (local time) to aggregate in the format "YYYY-MM-DD HH:MM:SS"
start_time <- "2018-01-01 00:00:00"
## End date (local time) to aggregate in the format "YYYY-MM-DD HH:MM:SS"
end_time <- "2018-01-19 00:00:00"

############
## minimum number of available 6min QPE to aggregate.
## If the available QPE files are below this number no aggregation will be made
min_scan <- 8

##########
# Note
# precipitation at 20 hr are the aggregated precipitation from 20:00:00 to 20:59:59

########################################## End Edit #############################################
library(ncdf4)
library(parallel)
library(foreach)
library(doParallel)

nb.cores <- detectCores()-1
okpar <- if(nb.cores < 3) FALSE else TRUE
packages <- "ncdf4"

##################
dir.6min.nc <- file.path(radar_data_dir, "QPE")
dir.1hr.nc <- file.path(radar_data_dir, "Aggregated_QPE_1hr")
if(!dir.exists(dir.6min.nc)) stop("The folder containing the 6min data does not found\n")
if(!dir.exists(dir.1hr.nc)) stop("The folder to put the hourly aggregated QPE does not found\n")
cat("Aggregate 6min QPE to hourly .........\n")

all.nc <- list.files(dir.6min.nc, ".nc", full.names = TRUE, recursive = TRUE)
if(length(all.nc) == 0) stop("No data to process")
all.nc <- all.nc[1]

nc <- nc_open(all.nc)
x <- nc$dim[[1]]$vals
y <- nc$dim[[2]]$vals
nc_close(nc)
dx <- ncdim_def("Lon", "degreeE", x)
dy <- ncdim_def("Lat", "degreeN", y)
rrout <- ncvar_def("precip", "mm", list(dx, dy), -99, longname = "Radar estimated rainfall",
				   prec = "float", compression = 9)

###################
start_time <- as.POSIXct(start_time, tz = "Africa/Kigali")
end_time <- as.POSIXct(end_time, tz = "Africa/Kigali")

###################
daty_gmt <- list.files(dir.6min.nc)
daty_gmt <- daty_gmt[grepl("^[[:digit:]]*$", daty_gmt)]
if(length(daty_gmt) == 0) stop("No data to process")

daty_conv <- lapply(daty_gmt, function(x){
	ncfiles <- list.files(file.path(dir.6min.nc, x), ".nc")
	ncfiles <- gsub("[^[:digit:]]", "", ncfiles)
	utc <- as.POSIXct(strptime(ncfiles, "%Y%m%d%H%M%S", tz = "GMT"))
	kigali <- as.POSIXct(format(utc, tz = "Africa/Kigali"), tz = "Africa/Kigali")
	data.frame(utc, kigali)
})
daty_conv <- do.call(rbind, daty_conv)
if(nrow(daty_conv) == 0) stop("No data to process")

id.time <- daty_conv[, 2] >= start_time & daty_conv[, 2] <= end_time
daty_conv <- daty_conv[id.time, , drop = FALSE]
index <- split(seq(nrow(daty_conv)), list(cut(as.POSIXct(daty_conv[, 2])-1, "hour")))

if(okpar & length(index) >= 24){
	klust <- makeCluster(nb.cores)
	registerDoParallel(klust)
	`%dofun%` <- `%dopar%`
	closeklust <- TRUE
}else{
	klust <- NULL
	`%dofun%` <- `%do%`
	closeklust <- FALSE
}

ret <- foreach(ii = seq_along(index), .packages = packages) %dofun% {
	ix <- index[[ii]]
	if(length(ix) < min_scan) return(NULL)
	daty_hour <- format(as.POSIXct(names(index[ii]), tz = "Africa/Kigali"), "%Y%m%d_%H")
	gmt_dt <- daty_conv[ix, 1]
	# kigali_dt <- daty_conv[ix, 2]
	dir_day <- format(gmt_dt, "%Y%m%d")
	ncfiles <- paste0("precip_", format(gmt_dt, "%Y%m%d_%H%M%S"), ".nc")
	ncpath <- file.path(dir.6min.nc, dir_day, ncfiles)

	ncdata <- lapply(seq_along(ncpath), function(ff){
		nc <- nc_open(ncpath[[ff]])
		z <- ncvar_get(nc, varid = nc$var[[1]]$name)
		nc_close(nc)
		return(z)
	})

	tmp <- unlist(ncdata)
	tmp[is.na(tmp)] <- 0
	tmp <- relist(tmp, ncdata)
	ncdata <- Reduce('+', tmp)
	ncdata[ncdata == 0] <- -99 

	ncfile <- file.path(dir.1hr.nc, paste0("precip_", daty_hour, ".nc"))
	nc <- nc_create(ncfile, rrout)
	ncvar_put(nc, rrout, ncdata)
	nc_close(nc)
	rm(tmp, ncdata)
	return(0)
}
if(closeklust) stopCluster(klust)
cat("Aggregating 6min QPE to hourly finished successfully\n")
