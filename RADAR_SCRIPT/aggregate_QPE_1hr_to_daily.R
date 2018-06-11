
### Path to the folder RADAR_DATA
radar_data_dir <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/RADAR_DATA"

##############
## Start date to aggregate in the format "YYYY-MM-DD"
start_time <- "2018-01-01"
## End date to aggregate in the format "YYYY-MM-DD"
end_time <- "2018-01-05"

############
## minimum number of available hourly QPE to aggregate.
## If the available QPE files are below this number no aggregation will be made
min_hour <- 22

##########
# Note
# precipitation at 2014-01-23 are computed using hourly data from 2014-01-23 08:00:00 to 2014-01-24 07:00:00

########################################## End Edit #############################################
library(ncdf4)

################## 

get.index.1hrto1day <- function(dates, precip = FALSE){
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

dir.1hr.adj <- file.path(radar_data_dir, "Aggregated_QPE_1hr")
dir.daily.adj <- file.path(radar_data_dir, "Aggregated_QPE_daily")
if(!dir.exists(dir.1hr.adj)) stop("The folder containing the 1hr QPE does not found\n")
if(!dir.exists(dir.daily.adj)) stop("The folder to put the aggregated daily QPE does not found\n")
cat("Aggregate hourly QPE to daily .........\n")

all.nc <- list.files(dir.1hr.adj, ".nc", full.names = TRUE, recursive = TRUE)
if(length(all.nc) == 0) stop("No data to process")
all.nc <- all.nc[1]

nc <- nc_open(all.nc)
x <- nc$dim[[1]]$vals
y <- nc$dim[[2]]$vals
nc_close(nc)
dx <- ncdim_def("Lon", "degreeE", x)
dy <- ncdim_def("Lat", "degreeN", y)
rrout <- ncvar_def("precip", "mm", list(dx, dy), -99, longname = "Daily radar estimated rainfall",
				   prec = "float", compression = 9)

###################
start_time <- as.Date(start_time)
end_time <- as.Date(end_time)

all.nc <- list.files(dir.1hr.adj, ".nc")
dates <- strptime(gsub("[^[:digit:]]", "", all.nc), "%Y%m%d%H", tz = "Africa/Kigali")
index <- get.index.1hrto1day(dates, precip = TRUE)
daty <- as.Date(names(index), "%Y%m%d")
idaty <- daty >= start_time & daty <= end_time
if(!any(idaty)) stop("No data to process")
daty <- daty[idaty]
index <- index[idaty]
idaty <- sapply(index, length) >= min_hour
if(!any(idaty)) stop("No data to process")
daty <- daty[idaty]
index <- index[idaty]

ret <- lapply(seq_along(index), function(ii){
	ix <- index[[ii]]
	ncpath <- file.path(dir.1hr.adj, all.nc[ix])

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

	ncfile <- file.path(dir.daily.adj, paste0("precip_", format(daty[ii], "%Y%m%d"), ".nc"))
	nc <- nc_create(ncfile, rrout)
	ncvar_put(nc, rrout, ncdata)
	nc_close(nc)
	rm(tmp, ncdata)
	return(0)
})

cat("Aggregating hourly QPE to daily finished successfully\n")
