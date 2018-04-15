library(ncdf4)
library(fields)
library(sp)
library(reshape2)
library(gstat)
library(automap)

##################################

reshapeXYZ2Matrix <- function(df){
	df <- as.data.frame(df, stringsAsFactors = FALSE)
	names(df) <- c('x', 'y', 'z')
	x <- sort(unique(df$x))
	y <- sort(unique(df$y))
	z <- acast(df, x~y, value.var = "z")
	dimnames(z) <- NULL
	return(list(x = x, y = y, z = z))
}

smooth.matrix <- function(mat, ns){
	matrix.shift <- function(n, shift){
		mshift <- matrix(NA, ncol = n, nrow = 2*shift+1)
		mshift[shift+1, ] <- seq(n)
		for(j in 1:shift){
			is <- shift-(j-1)
			mshift[j, ] <- c(tail(seq(n), -is), rep(NA, is))
			mshift[2*(shift+1)-j, ] <- c(rep(NA, is), head(seq(n), -is))
		}
		mshift
	}
	icol <- matrix.shift(ncol(mat), ns)
	matCol <- lapply(1:nrow(icol), function(j) mat[, icol[j, ]])
	irow <- matrix.shift(nrow(mat), ns)
	matRow <- lapply(1:nrow(irow), function(j) mat[irow[j, ], ])
	matEns <- c(matCol, matRow)
	matEns <- simplify2array(matEns)
	res <- apply(matEns, 1:2, mean, na.rm = TRUE)
	res[is.nan(res)] <- NA
	return(res)
}

get.neighbours.ix <- function(pts.coords, grid.coords, padxy){
	spxycrd <- expand.grid(grid.coords)
	coordinates(spxycrd) <- ~x+y
	spxycrd <- SpatialPixels(points = spxycrd, tolerance = sqrt(sqrt(.Machine$double.eps)), proj4string = CRS(as.character(NA)))
	nxy <- spxycrd@grid@cellsize
	voisin <- lapply(seq_along(pts.coords[['x']]), function(j){
					xy <- expand.grid(x = pts.coords[['x']][j] + nxy[1]*(-padxy[1]:padxy[1]),
										y = pts.coords[['y']][j] + nxy[2]*(-padxy[2]:padxy[2]))
					coordinates(xy) <- ~x+y
					if(length(xy) > 1) xy <- SpatialPixels(points = xy, tolerance = sqrt(sqrt(.Machine$double.eps)))
					return(xy)
				})
	ij2xtr <- lapply(voisin, over, y = spxycrd)
	ij2xtr <- lapply(ij2xtr, function(x) x[!is.na(x)])
	# ij2xtr <- Map(Filter, list(Negate(is.na)), ij2xtr)
	return(ij2xtr)
}

get.neighbours.values <- function(grid.values, index, fun){
	fun <- match.fun(fun)
	ix <- unlist(index)
	grd <- relist(grid.values[ix], index)
	grd <- sapply(grd, function(x){
		x <- x[!is.na(x)]
		if(length(x) == 0) return(NA)
		fun(x)
	})
	return(grd)
}

AdjustAdd <- function(point.data, grid.data, padxy = c(2, 2),
					fun = 'median', mfb = NULL, mingages = 5, minval = 0, ...)
{
	locations <- do.call(cbind.data.frame, point.data)
	coordinates(locations) <- ~x+y
	## check padxy
	index <- get.neighbours.ix(point.data[c('x', 'y')], grid.data[c('x', 'y')], padxy)
	locations$grd <- get.neighbours.values(grid.data[['z']], index, fun)
	ix <- !is.na(locations$z) & !is.na(locations$grd)
	locations <- locations[ix, ]
	im <- locations$z >= minval & locations$grd >= minval
	locations <- locations[im, ]
	if(length(locations) < mingages) return(grid.data)

	locations$res <- locations$z - locations$grd

	## add options to create grid
	interp.grid <- expand.grid(grid.data[c('x', 'y')])
	coordinates(interp.grid) <- ~x+y

	res.grd <- krige(res~1, locations = locations, newdata = interp.grid, debug.level = 0, ...)
	# resid <- matrix(res.grd$var1.pred, ncol = length(grid.data$y), nrow = length(grid.data$x))
	# resid[is.na(resid)] <- 0
	res.grd$var1.pred[is.na(res.grd$var1.pred)] <- 0
	res.image <- as.image(res.grd$var1.pred, x = coordinates(res.grd), nx = length(grid.data$x), ny = length(grid.data$y))
	resid <- image.smooth(res.image, theta= 0.01)
	resid <- resid$z

	out <- grid.data$z + resid
	out[out < 0] <- 0
	out <- smooth.matrix(out, 2)
	out <- c(grid.data[c('x', 'y')], z = list(out))
	return(out)
}

AdjustMultiply <- function(point.data, grid.data, padxy = c(2, 2),
							fun = 'median', mfb = NULL, mingages = 5, minval = 0.1, ...)
{
	locations <- do.call(cbind.data.frame, point.data)
	coordinates(locations) <- ~x+y
	## check padxy
	index <- get.neighbours.ix(point.data[c('x', 'y')], grid.data[c('x', 'y')], padxy)
	locations$grd <- get.neighbours.values(grid.data[['z']], index, fun)
	ix <- !is.na(locations$z) & !is.na(locations$grd)
	locations <- locations[ix, ]
	im <- locations$z >= minval & locations$grd >= minval
	locations <- locations[im, ]
	if(length(locations) < mingages) return(grid.data)

	locations$res <- locations$z / locations$grd

	## add options to create grid
	interp.grid <- expand.grid(grid.data[c('x', 'y')])
	coordinates(interp.grid) <- ~x+y

	res.grd <- krige(res~1, locations = locations, newdata = interp.grid, debug.level = 0, ...)
	resid <- matrix(res.grd$var1.pred, ncol = length(grid.data$y), nrow = length(grid.data$x))
	resid[is.na(resid)] <- 1

	out <- grid.data$z * resid
	out[out < 0] <- 0
	out <- smooth.matrix(out, 2)
	out <- c(grid.data[c('x', 'y')], z = list(out))
	return(out)
}

AdjustMixed <- function(point.data, grid.data, padxy = c(2, 2),
						fun = 'median', mfb = NULL, mingages = 5, minval = 0.1, ...)
{
	locations <- do.call(cbind.data.frame, point.data)
	coordinates(locations) <- ~x+y
	## check padxy
	index <- get.neighbours.ix(point.data[c('x', 'y')], grid.data[c('x', 'y')], padxy)
	locations$grd <- get.neighbours.values(grid.data[['z']], index, fun)
	ix <- !is.na(locations$z) & !is.na(locations$grd)
	locations <- locations[ix, ]
	im <- locations$z >= minval & locations$grd >= minval
	locations <- locations[im, ]
	if(length(locations) < mingages) return(grid.data)

	locations$eps <- (locations$z - locations$grd) / (locations$grd^2 + 1)
	locations$delta <- ((locations$z - locations$eps) / locations$grd) - 1

	## add options to create grid
	interp.grid <- expand.grid(grid.data[c('x', 'y')])
	coordinates(interp.grid) <- ~x+y

	eps.grd <- krige(eps~1, locations = locations, newdata = interp.grid, debug.level = 0, ...)
	# epsilon <- matrix(eps.grd$var1.pred, ncol = length(grid.data$y), nrow = length(grid.data$x))
	# epsilon[is.na(epsilon)] <- 0
	eps.grd$var1.pred[is.na(eps.grd$var1.pred)] <- 0
	eps.image <- as.image(eps.grd$var1.pred, x = coordinates(eps.grd), nx = length(grid.data$x), ny = length(grid.data$y))
	epsilon <- image.smooth(eps.image, theta= 0.01)
	epsilon <- epsilon$z

	delta.grd <- krige(delta~1, locations = locations, newdata = interp.grid, debug.level = 0, ...)
	# delta <- matrix(delta.grd$var1.pred, ncol = length(grid.data$y), nrow = length(grid.data$x))
	# delta[is.na(delta)] <- 0
	delta.grd$var1.pred[is.na(delta.grd$var1.pred)] <- 0
	delta.image <- as.image(delta.grd$var1.pred, x = coordinates(delta.grd), nx = length(grid.data$x), ny = length(grid.data$y))
	delta <- image.smooth(delta.image, theta= 0.01)
	delta <- delta$z

	out <- (1. + delta) * grid.data$z + epsilon
	out[out < 0] <- 0

	out <- smooth.matrix(out, 2)
	out <- c(grid.data[c('x', 'y')], z = list(out))
	return(out)
}

AdjustMFB <- function(point.data, grid.data, padxy = c(2, 2), fun = 'median',
					mfb = list(method = "linear", minslope = 0.1, minr = 0.5, maxp = 0.01),
					mingages = 5, minval = 0.1, nmin = NA, nmax = NA, maxdist = NA)
{
	locations <- do.call(cbind.data.frame, point.data)
	coordinates(locations) <- ~x+y
	## check padxy
	index <- get.neighbours.ix(point.data[c('x', 'y')], grid.data[c('x', 'y')], padxy)
	locations$grd <- get.neighbours.values(grid.data[['z']], index, fun)
	ix <- !is.na(locations$z) & !is.na(locations$grd)
	locations <- locations[ix, ]
	im <- locations$z >= minval & locations$grd >= minval
	locations <- locations[im, ]
	if(length(locations) < mingages) return(grid.data)

	ratios <- locations$z / locations$grd

	corrfact <- 1
	if(mfb$method == "mean") corrfact <- mean(ratios)
	if(mfb$method == "median") corrfact <- median(ratios)
	if(mfb$method == "linear"){
		linreg <- try(lm(locations$grd~locations$z), silent = TRUE)
		if(!inherits(linreg, "try-error")){
			slinr <- summary(linreg)
			pars.lin <- list(slope = slinr$coefficients[2, 1],
							 r = cor(locations$z, locations$grd),
							 p = slinr$coefficients[2, 4])
		}else pars.lin <- list(slope = 0, r = 0, p = Inf)

		if(pars.lin$slope > mfb$minslope & pars.lin$r > mfb$minr & pars.lin$p < mfb$maxp){
			lstsq <- try(lsfit(locations$z, locations$grd, intercept = FALSE), silent = TRUE)
			if(!inherits(linreg, "try-error")){
				slope <- lstsq$coefficients
				corrfact <- if(slope == 0) 1 else 1/slope
			}
		}
	}

	out <- corrfact * grid.data$z 
	out[out < 0] <- 0
	out <- smooth.matrix(out, 2)
	out <- c(grid.data[c('x', 'y')], z = list(out))
	return(out)
}

AdjustKED <- function(point.data, grid.data, padxy = c(2, 2),
          fun = 'median', mfb = NULL, mingages = 5, minval = 0, vgm.model = c("Sph", "Exp", "Gau"), ...)
{
	locations <- do.call(cbind.data.frame, point.data)
	coordinates(locations) <- ~x+y
	## check padxy
	index <- get.neighbours.ix(point.data[c('x', 'y')], grid.data[c('x', 'y')], padxy)
	locations$grd <- get.neighbours.values(grid.data[['z']], index, fun)
	ix <- !is.na(locations$z) & !is.na(locations$grd)
	locations <- locations[ix, ]
	im <- locations$z >= minval & locations$grd >= minval
	locations <- locations[im, ]
	if(length(locations) < mingages) return(grid.data)

	if(!is.null(vgm.model)){
		vgm <- try(autofitVariogram(z ~ grd, input_data = locations, model = vgm.model, cressie = TRUE), silent = TRUE)
		vgm <- if(!inherits(vgm, "try-error")) vgm$var_model else NULL
	}

	## add options to create grid
	interp.grid <- data.frame(expand.grid(grid.data[c('x', 'y')]), grd = c(grid.data$z))
	coordinates(interp.grid) <- ~x+y

	res.grd <- krige(z ~ grd, locations = locations, newdata = interp.grid, model = vgm, debug.level = 0, ...)
	out <- matrix(res.grd$var1.pred, ncol = length(grid.data$y), nrow = length(grid.data$x))
	out[is.infinite(out)] <- NA
	out[out < 0] <- 0
	out <- smooth.matrix(out, 2)
	out <- c(grid.data[c('x', 'y')], z = list(out))
	return(out)
}

