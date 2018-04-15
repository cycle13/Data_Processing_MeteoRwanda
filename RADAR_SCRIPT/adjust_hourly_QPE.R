
### Path to the folder RADAR_SCRIPT
radar_script_dir <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/RADAR_SCRIPT"

### Path to the folder RADAR_DATA
radar_data_dir <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/RADAR_DATA"

### Path to the folder AWS_DATA
aws_data_dir <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/AWS_DATA"

##############
### Method to be used to adjust the QPE
## "ADD": additive error model
## "MUL": multiplicative error model
## "MIX": mixed error model
## "MFB": mean field bias
## "KED": kriging with external drift

adjust.Method <- "KED"

##############
## Start date (local time) to adjust in the format "YYYY-MM-DD HH:MM:SS"
start_time <- "2018-01-04 08:00:00"
## End date (local time) to adjust in the format "YYYY-MM-DD HH:MM:SS"
end_time <- "2018-01-05 08:00:00"

########################################## End Edit #############################################
library(parallel)
library(foreach)
library(doParallel)

if(!dir.exists(radar_script_dir)) stop("The folder containing the radar script does not found\n")
if(!dir.exists(radar_data_dir)) stop("The folder containing the radar data does not found\n")
if(!dir.exists(aws_data_dir)) stop("The folder containing the AWS  data does not found\n")
source(file.path(radar_script_dir, "adjust_QPE_functions.R"))
cat("Adjust hourly QPE with hourly AWS data .........\n")

nb.cores <- detectCores()-1
okpar <- if(nb.cores < 3) FALSE else TRUE
packages <- c("ncdf4", "matrixStats", "fields", "sp", "reshape2", "gstat", "automap")
toExports <- c("get.neighbours.ix", "smooth.matrix", "get.neighbours.values", "AdjustAdd",
				"AdjustMultiply", "AdjustMixed", "AdjustMFB", "AdjustKED")
##############
start_time <- as.POSIXct(start_time, tz = "Africa/Kigali")
end_time <- as.POSIXct(end_time, tz = "Africa/Kigali")
daty <- seq(start_time, end_time, "hour")

#############
adj.fun <- match.fun(switch(adjust.Method, 
					"ADD" = "AdjustAdd",
					"MUL" = "AdjustMultiply",
					"MIX" = "AdjustMixed", 
					"MFB" = "AdjustMFB", 
					"KED" =  "AdjustKED"))
mfb <- if(adjust.Method == "MFB") list(method = "mean") else NULL

#############

if(okpar & length(daty) >= 10){
	klust <- makeCluster(nb.cores)
	registerDoParallel(klust)
	`%dofun%` <- `%dopar%`
	closeklust <- TRUE
}else{
	klust <- NULL
	`%dofun%` <- `%do%`
	closeklust <- FALSE
}

ret <- foreach(ii = seq_along(daty), .packages = packages, .export = toExports) %dofun% {
	file.aws <- file.path(aws_data_dir, "AWS_1hr_Adjust_Radar", paste0("aws_", format(daty[ii], "%Y%m%d_%H"), ".csv"))
	file.rad <- file.path(radar_data_dir, "Aggregated_QPE_1hr", paste0("precip_", format(daty[ii], "%Y%m%d_%H"), ".nc"))
	if(!file.exists(file.aws)) return(NULL)
	if(!file.exists(file.rad)) return(NULL)
	stn <- read.table(file.aws, sep = ',')
	pts.aws <- list(x = stn[, 2], y = stn[, 3], z = stn[, 4])

	nc <- nc_open(file.rad)
	rad.data <- list(x = nc$dim[[1]]$vals, y = nc$dim[[2]]$vals,
					z = ncvar_get(nc, varid = nc$var[[1]]$name))
	nc_close(nc)

	adj.res <- adj.fun(pts.aws, rad.data, padxy = c(2, 2), fun = 'median', mfb = mfb, mingages = 5, minval = 0.1, nmin = 2, nmax = 4, maxdist = 0.15)
	adj.res$z[is.na(adj.res$z)] <- -99

	dx <- ncdim_def("Lon", "degreeE", adj.res$x)
	dy <- ncdim_def("Lat", "degreeN", adj.res$y)
	rrout <- ncvar_def("precip", "mm", list(dx, dy), -99, longname = "Hourly adjusted radar estimated rainfall",
					   prec = "float", compression = 9)

	ncfile <- file.path(radar_data_dir, "Adjusted_QPE_1hr", paste0("precip_", format(daty[ii], "%Y%m%d_%H"), ".nc"))
	nc <- nc_create(ncfile, rrout)
	ncvar_put(nc, rrout, adj.res$z)
	nc_close(nc)

	return(0)
}
if(closeklust) stopCluster(klust)

cat("Adjust hourly QPE finished successfully\n")

