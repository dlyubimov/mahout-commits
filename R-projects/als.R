
als.alsreg <- function(a, k, regrate, iter = 20) {
	
	a <- as.matrix(a)
	m <- nrow(a)
	n <- ncol(a)
	
	ut <- matrix(rnorm(k * m, sd = 0.1), nrow = k, ncol = m)
	# vt <- matrix(rnorm(k*n, sd=0.1),nrow=k, ncol=n);
	
	reg <- diag(regrate, k)
	
	for (i in 1:iter) {
		vt <- solve(ut %*% t(ut) + reg) %*% (ut %*% a)
		ut <- solve(vt %*% t(vt) + reg) %*% (vt %*% t(a))
	}
	
	
	res <- list()
	res$u <- t(ut)
	res$v <- t(vt)
	
	return(res)
	
}

#Sequential version of ALS-WR
als.alswr <- function(a, k, regrate, iter = 20) {
	
	a <- as.matrix(a)
	m <- nrow(a)
	n <- ncol(a)
	
	ut <- matrix(rnorm(k * m, sd = 0.1), nrow = k, ncol = m)
	vt <- matrix(rep(0, k * n), nrow = k, ncol = n)
	
	#TODO: figure out count normalization
	nu <- rowSums(matrix(as.numeric(a != 0), nrow = nrow(a), 
		ncol = ncol(a)))
	nv <- colSums(matrix(as.numeric(a != 0), nrow = nrow(a), 
		ncol = ncol(a)))
	nu <- nu/mean(nu)
	nv <- nv/mean(nv)
	
	reg <- diag(regrate, k)
	
	for (it in 1:iter) {
		for (i in 1:n) {
			if (nv[i] > 0) {
				# indices of a[,i] where it is non-zero
				uind <- (1:m)[a[, i] != 0]
				# form submatrix of u with rows in uind
				utsubm <- as.matrix(ut[, uind])
				utu <- utsubm %*% t(utsubm)
				vt[, i] <- solve(utu + nv[i] * reg) %*% (utsubm %*% 
				a[uind, i])
			}
		}
		
		for (i in 1:m) {
			if (nu[i] > 0) {
				#indices of a[i,] where non-zero entries are found
				vind <- (1:n)[a[i, ] != 0]
				vtsubm <- as.matrix(vt[, vind])
				
				vtv <- vtsubm %*% t(vtsubm)
				
				ut[, i] <- solve(vtv + nu[i] * reg) %*% (vtsubm %*% 
				a[i, vind])
			}
		}
	}
	
	
	res <- list()
	res$u <- t(ut)
	res$v <- t(vt)
	
	return(res)
	
}

als.extrSValues <- function ( u, v ) { 
  return (sqrt(apply(u,2,function(x) sum(x*x))*
	apply(v,2,function(x) sum(x*x))))
}

# test 
als.test1 <- function() {
	
	n <- 100
	m <- 200
	k <- 10
	regrate <- 1e-06
	iter <- 20
	
	#simulated input
	svalsim <- diag(k:1)
	
	usim <- qr.Q(qr(matrix(rnorm(m * k, mean = 3), nrow = m, 
		ncol = k)))
	vsim <- qr.Q(qr(matrix(rnorm(n * k, mean = 5), nrow = n, 
		ncol = k)))
	
	a <- usim %*% svalsim %*% t(vsim)
	
	#sparsify
	for (i in 1:m) for (j in 1:n) if (runif(1) < 0.97) 
		a[i, j] = 0
	
	#res <- als.alswr(a, k, regrate, iter)
	
	res <- als.alsreg ( a, k, regrate,iter )
	svalues <-als.extrSValues(res$u,res$v)
	
	
	#error per element
	sum(a - res$u %*% t(res$v))/(ncol(a) * nrow(a))
	
}

als.testRegRate2 <- function () { 
	set.seed(250)
	len <- 24 
	x <- runif(len)
	y <- x^3 + rnorm(len, 0, 0.06)
	ds <- data.frame(x=x,y=y)
	
	str(ds)
	
	
#	m <- nls(y~I(x^2)+I(x^3), data =ds, trace=T)
	m<-lm(y~x+I(x^2),ds )
	xmin<- -m$coefficients[2] / 2 / m$coefficients[3]

	
	plot (y ~ x, main="Known cubic, with noise")
	s <- seq(0,1,length=1000)
	lines (s, s^3, lty=2, col="green")
	lines (s, predict(m, list(x=s)), lty=1, col='blue')
	abline(v=xmin )
	
}
