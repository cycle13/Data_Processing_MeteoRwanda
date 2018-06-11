## Path to python.exe
# python.exe <- "C:/ProgramData/Anaconda3/python.exe"
python.exe <- "/Users/rijaf/anaconda/envs/wradlib/bin/python"

#############
## Path to the folder containing the radar cartesian mdv files
dir.radar.mdv <- "/Volumes/SeagateBackup/20180112_radar_data/FIDEL/radarcart/ops"

#############
## Path to the folder RADAR_SCRIPT
radar_script_dir <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/RADAR_SCRIPT"

## Path to the folder RADAR_DATA
radar_data_dir <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/RADAR_DATA"

##############
## Start date (GMT time) to process in the format "YYYYMMDD:HH"
start_day <- "20171004:17"
## End  date (GMT time) to process in the format "YYYYMMDD:HH"
# end_day <- "20180119:07"
end_day <- "20171004:17"

##############
## Marshall-Palmer coefficients
ZR_coeff_a <- 300
ZR_coeff_b <- 1.4
## Maximum value (Hail threshold)
Hail_threshold <- 60
## Minimum value to be taken account (Low dBZ threshold)
Low_dBZ_threshold <- 20

# ZR_coeff_a <- 200
# ZR_coeff_b <- 1.6
# Hail_threshold <- 55
# Low_dBZ_threshold <- 10

########################################## End Edit #############################################
library(fields)
library(ncdf4)
library(matrixStats)
library(parallel)
library(foreach)
library(doParallel)

python.script <- file.path(radar_script_dir, "read_radarCartesian.py")
dir.out.nc <- file.path(radar_data_dir, "QPE")
if(!file.exists(python.exe)) stop("python.exe does not find\n")
if(!file.exists(python.script)) stop("The script 'read_radarCartesian.py' to read the mdv files does not found\n")
if(!dir.exists(dir.radar.mdv)) stop("The folder containing the radar cartesian mdv files does not found\n")
if(!dir.exists(dir.out.nc)) stop("The folder put the converted QPE does not found\n")

nb.cores <- detectCores()-1
okpar <- if(nb.cores < 3) FALSE else TRUE
packages <- c("ncdf4", "matrixStats")
if((Sys.info()["sysname"] == "Windows") & okpar) cat("Allows Windows to use all processor cores")
separator <- if(Sys.info()["sysname"] == "Windows") ";" else ":"
virtualenv.path <- dirname(python.exe)

start_date <- strsplit(start_day, ":")[[1]][1]
start_hour <- as.numeric(strsplit(start_day, ":")[[1]][2])
end_date <- strsplit(end_day, ":")[[1]][1]
end_hour <- as.numeric(strsplit(end_day, ":")[[1]][2])

daty <- seq(as.Date(start_date, "%Y%m%d"), as.Date(end_date, "%Y%m%d"), 'day')
daty <- format(daty, "%Y%m%d")

convert_to_geographic <- TRUE

cat("Convert dBZ to QPE .........\n")

for(dd in seq_along(daty)){
	dirday <- file.path(dir.radar.mdv, daty[dd])
	if(!dir.exists(dirday)) next

	all.mdv <- list.files(dirday, ".mdv")
	if(length(all.mdv) == 0) next

	all.mdv <- gsub("[^[:digit:]]", "", all.mdv)
	all.mdv <- all.mdv[nchar(all.mdv) == 6]
	if(length(all.mdv) == 0) next

	hour <- as.numeric(substr(all.mdv, 1, 2))
	if(dd == 1) all.mdv <- all.mdv[hour >= start_hour]
	if(dd == length(daty)) all.mdv <- all.mdv[hour <= end_hour]
	if(length(all.mdv) == 0) next

	diroutday <- file.path(dir.out.nc, daty[dd])
	if(!dir.exists(diroutday)) dir.create(diroutday, recursive = TRUE, showWarnings = FALSE)

	if(convert_to_geographic){
		cat("Convert Cartesian to Geographic coordinates\n")
		mdvfile0 <- file.path(dirday, paste0(all.mdv[1], ".mdv"))
		ncfile0 <- file.path(diroutday, paste0(daty[dd], all.mdv[1], ".nc"))
		system2(python.exe, args = c(python.script, mdvfile0, ncfile0, virtualenv.path, separator), stdout = NULL, wait = TRUE)
		nc <- nc_open(ncfile0)
		x <- nc$dim[['x']]$vals
		y <- nc$dim[['y']]$vals
		lat0 <- ncvar_get(nc, varid = "origin_latitude")
		lon0 <- ncvar_get(nc, varid = "origin_longitude")
		nc_close(nc)

		## cartesian to geographique
		lon0 <- as.numeric(lon0)
		lat0 <- as.numeric(lat0)
		lat0.rad <- lat0*pi/180
		lon0.rad <- lon0*pi/180
		nx <- length(x)
		ny <- length(y)
		x <- matrix(x, nrow = nx, ncol = ny)
		y <- matrix(y, nrow = nx, ncol = ny, byrow = TRUE)

		rho <- sqrt(x * x + y * y)
		cc <- rho / 6370997
		sin.c <- sin(cc)
		cos.c <- cos(cc)

		lat.rad <- asin(cos.c * sin(lat0.rad) + y * sin.c * cos(lat0.rad) / rho)
		lat.deg <- lat.rad*180/pi
		lat.deg[rho == 0] <- lat0

		x1 <- x * sin.c
		x2 <- rho * cos(lat0.rad) * cos.c - y * sin(lat0.rad) * sin.c
		lon.rad <- lon0.rad + atan2(x1, x2)
		lon.deg <- lon.rad*180/pi
		lon.deg[lon.deg > 180] <- lon.deg[lon.deg > 180] - 360
		lon.deg[lon.deg < -180] <- lon.deg[lon.deg < -180] + 360

		## regrid data
		# xycoords <- cbind(c(lon.deg), c(lat.deg))
		# XY.grd <- as.image(rep(0, nrow(xycoords)), x = xycoords, nx = nx, ny = ny)
		# lon <- XY.grd$x
		# lat <- XY.grd$y
		lon <- lon.deg[, floor(ny/2)]
		lat <- lat.deg[floor(nx/2), ]
		rm(lon.deg, lon.rad, x1, x2, x, lat.deg, lat.rad, y, cos.c, sin.c, cc, rho)

		dx <- ncdim_def("Lon", "degreeE", lon)
		dy <- ncdim_def("Lat", "degreeN", lat)
		rrout <- ncvar_def("precip", "mm", list(dx, dy), -99, longname = "Radar estimated rainfall",
						   prec = "float", compression = 9)
		convert_to_geographic <- FALSE
	}

	if(okpar & length(all.mdv) >= 10){
		klust <- makeCluster(nb.cores)
		registerDoParallel(klust)
		`%dofun%` <- `%dopar%`
		closeklust <- TRUE
	}else{
		klust <- NULL
		`%dofun%` <- `%do%`
		closeklust <- FALSE
	}
	cat(paste("Processing day", daty[dd]), "\n")

	ret <- foreach(jj = seq_along(all.mdv), .packages = packages) %dofun% {
		mdvfile <- file.path(dirday, paste0(all.mdv[jj], ".mdv"))
		ncfile <- file.path(diroutday, paste0(daty[dd], all.mdv[jj], ".nc"))
		system2(python.exe, args = c(python.script, mdvfile, ncfile, virtualenv.path, separator), stdout = NULL, wait = TRUE)

		nc <- nc_open(ncfile)
		# z <- nc$dim[['z']]$vals
		init_time <- strsplit(nc$dim[['time']]$units, " ")[[1]][3]
		time <- as.POSIXct(nc$dim[['time']]$vals, origin = init_time, tz = "GMT")
		# interval <- as.numeric(nc$dim[['time']]$vals)
		interval <- 300
		dbz <- ncvar_get(nc, varid = "DBZ_F")
		nc_close(nc)

		dbz <- dbz[, , 5:35]
		dbz_F <- dbz
		dim(dbz_F) <- c(prod(dim(dbz)[1:2]), dim(dbz)[3])
		dbz_F <- t(dbz_F)
		dbz_F <- colMaxs(dbz_F, na.rm = TRUE)
		dbz_F[is.infinite(dbz_F)] <- NA
		dbz_F[dbz_F > Hail_threshold] <- Hail_threshold
		dbz_F[dbz_F < Low_dBZ_threshold] <- NA
		dim(dbz_F) <- dim(dbz)[1:2]

		Z <- 10^(dbz_F/10)
		rate <- (Z/ZR_coeff_a)^(1/ZR_coeff_b)
		precip <- rate * interval / 3600
		precip[precip < 0.0001] <- NA
		precip[is.na(precip)] <- -99

		ncout <- file.path(diroutday, paste0("precip_", daty[dd], "_", all.mdv[jj], ".nc"))
		nc <- nc_create(ncout, rrout)
		ncvar_put(nc, rrout, precip)
		nc_close(nc)
		rm(dbz, dbz_F, Z, rate, precip)
		unlink(ncfile)
		return(0)
	}
	if(closeklust) stopCluster(klust)
}

cat("Converting dBZ to QPE finished successfully\n")
