
nx_ny_as.image <- function(x) round(x / (0.0167323 * x^0.9602))

getBoundaries<-function(map){
	lxx <- as(map, "SpatialLines")
	lns <- slot(lxx, "lines")
	olns <- lapply(lns, slot, "Lines")
	ocrds <- matrix(nrow=0, ncol=2)
	for (i in seq(along=olns)) {
	   for (j in seq(along=olns[[i]])) {
		 crds <- rbind(slot(olns[[i]][[j]], "coords"), c(NA, NA))
		 ocrds <- rbind(ocrds, crds)
	   }
	}
	return(ocrds)
}

LatLonAxisLabels <- function(axis.x, axis.y){
	axis.x <- ifelse(axis.x > 180, -360 + axis.x, axis.x)
	sym1 <- ifelse(axis.x < 0 & axis.x > -180, 'W', ifelse(axis.x > 0 & axis.x < 180, 'E', ''))
	sym1 <- paste("paste(", paste(abs(axis.x), "*degree"), ",", sym1, ")", collapse = ',')
	lon_lab <- eval(parse(text = paste("expression(", sym1, ")", sep = "")))
	sym2 <- ifelse(axis.y < 0, 'S', ifelse(axis.y > 0, 'N', ''))
	sym2 <- paste("paste(", paste(abs(axis.y), "*degree"), ",", sym2, ")", collapse = ',')
	lat_lab <- eval(parse(text = paste("expression(", sym2, ")", sep = "")))
	return(list(xaxl = lon_lab, yaxl = lat_lab))
}

LatLonAxisLabels1 <- function(axis.x, axis.y){
	axis.x <- ifelse(axis.x > 180, -360 + axis.x, axis.x)
	sym1 <- ifelse(axis.x < 0 & axis.x > -180, 'W', ifelse(axis.x > 0 & axis.x < 180, 'E', ''))
	lon_lab <- paste(abs(axis.x), "°", sym1, sep = '')
	sym2 <- ifelse(axis.y < 0, 'S', ifelse(axis.y > 0, 'N', ''))
	lat_lab <- paste(abs(axis.y), "°", sym2, sep = '')
	return(list(xaxl = lon_lab, yaxl = lat_lab))
}
