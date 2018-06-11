
### Path to the folder AWS_DATA
AWS_DATA_DIR <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/AWS_DATA"
### Path to the folder AWS_SCRIPT
AWS_SCRIPT_DIR <- "~/Desktop/ECHANGE/github/Data_Processing_MeteoRwanda/AWS_SCRIPT"

### File to save plot in JPEG
polt.jpeg <- "~/Desktop/Daily_Rainfall.jpg"

########################################## End Edit #############################################
library(stringr)
library(jsonlite)

plt_pars <- file.path(AWS_SCRIPT_DIR, 'params', 'plot_time_series.json')
plt_pars <- fromJSON(plt_pars)
source(file.path(AWS_SCRIPT_DIR, "getAWS_TimeSeries.R"))
if(str_trim(plt_pars$Tstep) == "10day"){
	dek <- as.numeric(substr(daty0, 7, 7))
	dek <- ifelse(dek == 1, "05", ifelse(dek == 2, 15, 25))
	daty0 <- paste0(substr(daty0, 1, 6), dek)
} 
daty <- strptime(daty0, format.limit, tz = "Africa/Kigali")

kolor <- rainbow(ncol(DATA.AWS))
ylim <- range(pretty(DATA.AWS))
ylab <- switch(str_trim(plt_pars$var), 
			"RR" = "Rainfall (mm)",
			"TN" = "Minimum Temperature (C)",
			"TM" = "Mean Temperature (C)",
			"TX" = "Maximum Temperature (C)")

temps <- switch(str_trim(plt_pars$Tstep),
				"10min" = "10 minutes",
				"1hr" = "Hourly",
				"1day" = "Daily",
				"10day" = "Dekadal")
ylab <- paste(temps, ylab)

jpeg(polt.jpeg, width = 11, height = 8, units = "in", res = 400)
layout( matrix(1:2, ncol = 1), widths = 1, heights = c(0.9, 0.2), respect = FALSE)
op <- par(mar = c(3.5, 6.5, 2.5, 2.1))
plot(daty, DATA.AWS[, 1], type = 'n', ylim = ylim, xlab = "", ylab = ylab)
abline(v = axTicks(1), col = "lightgray", lty = "dotted")
abline(h = axTicks(2), col = "lightgray", lty = "dotted")
for(j in seq(ncol(DATA.AWS))) lines(daty, DATA.AWS[, j], type = 'o', lwd = 1.5, col = kolor[j], pch = 21, bg = kolor[j], cex = 0.7)
par(op)
op <- par(mar = c(1, 6.5, 0, 2.1))
plot.new()
legend("center", "groups", legend = STN.NAME, col = kolor, lwd = 3, ncol = 5, cex = 0.8)
par(op)
dev.off()



