
### Path to the folder AWS_DATA
AWS_DATA_DIR <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/AWS_DATA"
### Path to the folder AWS_SCRIPT
AWS_SCRIPT_DIR <- "/Users/rijaf/Desktop/ECHANGE/github/Data_Processing/AWS_SCRIPT"

### File to save plot in JPEG
polt.jpeg <- "/Users/rijaf/Desktop/map_Daily_Rainfall.jpg"

########################################## End Edit #############################################
library(stringr)
library(jsonlite)
library(rgdal)
library(fields)

plt_pars <- file.path(AWS_SCRIPT_DIR, 'params', 'plot_spatial_map.json')
plt_pars <- fromJSON(plt_pars)
source(file.path(AWS_SCRIPT_DIR, "getAWS_SpatialData.R"))
source(file.path(AWS_SCRIPT_DIR, "map_functions.R"))
file.shp <- paste0("RWA_adm", plt_pars$shp)
shpf <- file.path(AWS_DATA_DIR, "shapefiles")
map <- readOGR(dsn = shpf, layer = file.shp, verbose = FALSE)
ocrds <- getBoundaries(map)

leglab <- switch(str_trim(plt_pars$var), 
			"RR" = "Rainfall (mm)",
			"TN" = "Minimum Temperature (C)",
			"TM" = "Mean Temperature (C)",
			"TX" = "Maximum Temperature (C)")

temps <- switch(str_trim(plt_pars$Tstep),
				"10min" = "10 minutes",
				"1hr" = "Hourly",
				"1day" = "Daily",
				"10day" = "Dekadal")
leglab <- paste(temps, leglab)

xrmap <- range(ocrds[, 1], na.rm = TRUE)
yrmap <- range(ocrds[, 2], na.rm = TRUE)
nx <- nx_ny_as.image(diff(xrmap))
ny <- nx_ny_as.image(diff(yrmap))
tmp <- as.image(AWS.DAT$values, nx = nx, ny = ny, x = cbind(as.numeric(AWS.DAT$longitude), as.numeric(AWS.DAT$latitude)))

xlim <- c(min(c(xrmap, tmp$x)), max(c(xrmap, tmp$x)))
ylim <- c(min(c(yrmap, tmp$y)), max(c(yrmap, tmp$y)))

jpeg(polt.jpeg, width = 11, height = 8, units = "in", res = 400)
opar <- par(mar = c(7, 4, 2.5, 2.5))
plot(1, xlim = xlim, ylim = ylim, xlab = "", ylab = "", type = "n", xaxt = 'n', yaxt = 'n')
axlabsFun <- if(Sys.info()["sysname"] == "Windows") LatLonAxisLabels else LatLonAxisLabels1
axlabs <- axlabsFun(axTicks(1), axTicks(2))
axis(side = 1, at = axTicks(1), labels = axlabs$xaxl, tcl = -0.2, cex.axis = 0.8)
axis(side = 2, at = axTicks(2), labels = axlabs$yaxl, tcl = -0.2, las = 1, cex.axis = 0.8)

breaks <- pretty(tmp$z, n = 10, min.n = 5)
breaks <- if(length(breaks) > 0) breaks else c(0, 1)
kolor <- tim.colors(length(breaks)-1)
legend.args <- list(text = leglab, cex = 0.8, side = 1, line = 2)

abline(h = axTicks(2), v = axTicks(1), col = "lightgray", lty = 3)
lines(ocrds[, 1], ocrds[, 2], lwd = 1, col = "black")

image.plot(tmp, breaks = breaks, col = kolor, horizontal = TRUE,
			xaxt = 'n', yaxt = 'n', add = TRUE, legend.mar = 3.5,
			legend.width = 0.7, legend.args = legend.args,
			axis.args = list(at = breaks, labels = breaks, cex.axis = 0.7,
			font = 2, tcl = -0.3, mgp = c(0, 0.5, 0)))
par(opar)
dev.off()

