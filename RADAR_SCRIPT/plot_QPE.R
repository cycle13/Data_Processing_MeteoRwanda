
## Path to the folder RADAR_SCRIPT
radar_script_dir <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/RADAR_SCRIPT"

## Path to the folder RADAR_DATA
radar_data_dir <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/RADAR_DATA"

## data to plot
# Single scan: "QPE"
# One hour aggregated QPE: "Aggregated_QPE_1hr"
# One hour adjusted QPE: "Adjusted_QPE_1hr"
# Daily aggregated QPE: "Aggregated_QPE_daily"
# Daily adjusted QPE: "Adjusted_QPE_daily" (rename from "Aggregated_Adjusted_QPE_daily")
data_dir <- "Adjusted_QPE_1hr"

## date to plot in local time
# date format
# for single scan  "YYYYMMDDHHMM"
# for hourly "YYYYMMDDHH"
# for daily "YYYYMMDD"
date2plot <- "2018010417"

## shapefile for administrative delimitation
# Country: 0
# Province: 1
# District: 2
# Sector: 3
shp <- 2

### File to save plot in JPEG
polt.jpeg <- "/Users/rijaf/Desktop/QPE_onehour.jpg"


########################################## End Edit #############################################
library(stringr)
library(rgdal)
library(fields)
library(ncdf4)

source(file.path(radar_script_dir, "map_functions.R"))
file.shp <- paste0("RWA_adm", shp)
shpf <- file.path(radar_data_dir, "shapefiles")
map <- readOGR(dsn = shpf, layer = file.shp, verbose = FALSE)
ocrds <- getBoundaries(map)

dir.path <- file.path(radar_data_dir, data_dir)

if(data_dir%in%c("Aggregated_QPE_1hr", "Adjusted_QPE_1hr"))
	fileqpe <- paste0("precip_", substr(date2plot, 1, 8), "_", substr(date2plot, 9, 10), ".nc")
if(data_dir%in%c("Aggregated_QPE_daily", "Adjusted_QPE_daily"))
	fileqpe <- paste0("precip_", date2plot, ".nc")
if(data_dir == "QPE"){
	all.dates <- basename(list.files(dir.path, recursive = TRUE))
	all.dates <- gsub("[^[:digit:]]", "", all.dates)
	all.dates <-  as.POSIXct(strptime(all.dates, "%Y%m%d%H%M%S", tz = "GMT"))
	date.rw <- as.POSIXct(strptime(date2plot, "%Y%m%d%H%M%S", tz = "Africa/Kigali"))
	date.gmt <- as.POSIXct(format(date.rw, tz = "GMT"), tz = "GMT")
	date.gmt <- date.gmt + c(-250, 250)
	all.dates <- all.dates[all.dates >= date.gmt[1] & all.dates <= date.gmt[2]]
	if(length(all.dates) == 0) stop("No data found\n")
	date.gmt <- all.dates[which.min(abs(all.dates-date.gmt))]
	date2plot <- format(as.POSIXct(format(date.gmt, tz = "Africa/Kigali"), tz = "Africa/Kigali"), "%Y%m%d%H%M%S")
	date.gmt <- format(date.gmt, "%Y%m%d%H%M%S")
	fileqpe <- file.path(substr(date.gmt, 1, 8), paste0("precip_", substr(date.gmt, 1, 8), "_", substr(date.gmt, 9, 14), ".nc"))
}

file.qpe <- file.path(dir.path, fileqpe)
if(!file.exists(file.qpe)) stop("No data found\n")

nc <- nc_open(file.qpe)
x <- nc$dim[[1]]$vals
y <- nc$dim[[2]]$vals
z <- ncvar_get(nc, varid = nc$var[[1]]$name)
nc_close(nc)

titre <- switch(data_dir, 
			"QPE" = paste("QPE", date2plot, "local time"), 
			"Aggregated_QPE_1hr" = paste("Hourly QPE", date2plot, "local time"),
			"Adjusted_QPE_1hr" = paste("Hourly adjusted QPE with AWS", date2plot, "local time"),
			"Aggregated_QPE_daily" = paste("Daily QPE", date2plot),
			"Adjusted_QPE_daily" = paste("Daily adjusted QPE with AWS", date2plot))

xlim <- range(x)
ylim <- range(y)

jpeg(polt.jpeg, width = 11, height = 8, units = "in", res = 400)
opar <- par(mar = c(7, 4, 2.5, 2.5))
plot(1, xlim = xlim, ylim = ylim, xlab = "", ylab = "", type = "n", xaxt = 'n', yaxt = 'n')
axlabsFun <- if(Sys.info()["sysname"] == "Windows") LatLonAxisLabels else LatLonAxisLabels1
axlabs <- axlabsFun(axTicks(1), axTicks(2))
axis(side = 1, at = axTicks(1), labels = axlabs$xaxl, tcl = -0.2, cex.axis = 0.8)
axis(side = 2, at = axTicks(2), labels = axlabs$yaxl, tcl = -0.2, las = 1, cex.axis = 0.8)
title(main = titre, cex.main = 1, font.main= 2)

breaks <- pretty(z, n = 10, min.n = 5)
breaks <- if(length(breaks) > 0) breaks else c(0, 1)
kolor <- tim.colors(length(breaks)-1)
legend.args <- list(text = "Rainfall Estimate (mm)", cex = 0.8, side = 1, line = 2)

image.plot(x, y, z, breaks = breaks, col = kolor, horizontal = TRUE,
			xaxt = 'n', yaxt = 'n', add = TRUE, legend.mar = 3.5,
			legend.width = 0.7, legend.args = legend.args,
			axis.args = list(at = breaks, labels = breaks, cex.axis = 0.7,
			font = 2, tcl = -0.3, mgp = c(0, 0.5, 0)))
abline(h = axTicks(2), v = axTicks(1), col = "lightgray", lty = 3)
lines(ocrds[, 1], ocrds[, 2], lwd = 1, col = "black")

par(opar)
dev.off()



