
als.alsreg <- function(a, k, regrate, iter=20) { 

a <- as.matrix(a)
m <- nrow(a)
n <- ncol (a)

ut <- matrix(rnorm(k*m,sd=0.1),nrow=k,ncol=m);
# vt <- matrix(rnorm(k*n, sd=0.1),nrow=k, ncol=n);

reg <- diag(regrate,k)

for ( i in 1:iter ) { 
  vt <- solve(ut %*% t(ut) + reg) %*% ( ut %*% a )
  ut <- solve(vt %*% t(vt) + reg) %*% ( vt %*% t(a) )	
}


res <- list();
res$u <- t(ut);
res$v <- t(vt);

return (res);

}

#Sequential version of ALS-WR
als.alswr <- function(a, k, regrate, iter=20) { 

a <- as.matrix(a)
m <- nrow (a)
n <- ncol (a)

ut <- matrix(rnorm(k*m,sd=0.1),nrow=k,ncol=m);
vt <- matrix(rep(0,k*n), nrow=k, ncol=n);

#TODO: figure out count normalization
nu <- rowSums(matrix(as.numeric(a!=0),nrow=nrow(a),ncol=ncol(a)))
nv <- colSums(matrix(as.numeric(a!=0),nrow=nrow(a),ncol=ncol(a)))

reg <- diag(regrate,k)

	for ( it in 1:iter ) { 
		for ( i in 1:n ) { 
			if ( nv[i] > 0 ) { 
				# indices of a[,i] where it is non-zero
			    uind <- (1:length(a[,i]))[a[,i]!=0]
			    # form submatrix of u with rows in uind
			    utsubm <- as.matrix(ut[,uind])
			    utu <- utsubm %*% t(utsubm)  
			 	vt[,i] <- solve(utu + nv[i]*reg) %*% (utsubm %*% a[uind,i] )
			}
		} 
		
		for ( i in 1:m ) { 
			if ( nu[i] > 0 ) { 
				#indices of a[i,] where non-zero entries are found
				vind <- (1:length(a[i,]))[a[i,]!=0]
				vtsubm <- as.matrix(vt [, vind])
					
				vtv <- vtsubm %*% t(vtsubm)
				
				ut[,i] <- solve(vtv + nu[i]*reg) %*% (vtsubm %*% a[i,vind])
			}
		} 
	}


res <- list();
res$u <- t(ut);
res$v <- t(vt);

return (res);

}

# test 
ssvd.test <- function () { 

n<-1000
m<-2000
k<-10
regrate <- 0.000001
iter <- 20

#simulated input
svalsim<-diag(k:1)

usim<- qr.Q(qr(matrix(rnorm(m*k, mean=3), nrow=m,ncol=k)))
vsim<- qr.Q(qr( matrix(rnorm(n*k,mean=5), nrow=n,ncol=k)))

a <- usim %*% svalsim %*% t(vsim) 

#sparsify
for ( i in 1:m ) 
	for (j in 1:n ) 
		if ( runif(1)<0.97 ) a[i,j]=0;


#res <- ssvd.alsreg ( a, k, regrate,iter )
res <- als.alswr(a,k,regrate,iter)

#error per element
sum( a - res$u %*% t(res$v) )/ (ncol(a)* nrow(a))



}