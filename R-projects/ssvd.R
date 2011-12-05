
# standard SSVD
ssvd.svd <- function(x, k, p=25, qiter=0 ) { 

a <- as.matrix(x)
m <- nrow(a)
n <- ncol(a)
p <- min( min(m,n)-k,p)
r <- k+p

omega <- matrix ( rnorm(r*n), nrow=n, ncol=r)

y <- a %*% omega

q <- qr.Q(qr(y))

b<- t(q) %*% a

#power iterations
for ( i in 1:qiter ) { 
  y <- a %*% t(b)
  q <- qr.Q(qr(y))
  b <- t(q) %*% a
}

bbt <- b %*% t(b)

e <- eigen(bbt, symmetric=T)

res <- list()

res$svalues <- sqrt(e$values)[1:k]
uhat=e$vectors[1:k,1:k]

res$u <- (q %*% e$vectors)[,1:k]
res$v <- (t(b) %*% e$vectors %*% diag(1/e$values))[,1:k]

return(res)
}



#############
## ssvd with pci options
ssvd.cpci <- function ( x, k, p=25, qiter=0 ) { 

a <- as.matrix(x)
m <- nrow(a)
n <- ncol(a)
p <- min( min(m,n)-k,p)
r <- k+p


# compute median xi
xi<-rep(0,n)
for (i in 1:m ) xi <- xi + a[i,]
xi <- xi / m



omega <- matrix ( rnorm(r*n), nrow=n, ncol=r)

y <- a %*% omega

#fix y
xio = t(omega) %*% cbind(xi)
for (i in 1:r ) y[,i]<- y[,i]-xio[i]

#debug -- fixed a
#fixeda<- a
#for (i in 1:m) fixeda[i,]<- fixeda[i,]-xi
#sum(y-fixeda %*% omega)




q <- qr.Q(qr(y))

b<- t(q) %*% a

# compute sum of q rows 
qs <- rep(0,r)
for ( i in 1:r) qs[i] <- sum(q[,i])
qs <- cbind(qs)

#power iterations
for ( i in 1:qiter ) { 

  # fix b 
  b <- b - qs %*% rbind(xi) 

  y <- a %*% t(b)

  # fix y 
  xio = b %*% cbind(xi)
  for (i in 1:r ) y[,i]<- y[,i]-xio[i]

  q <- qr.Q(qr(y))
  b <- t(q) %*% a

  qs <- rep(0,r)
  for ( i in 1:r) qs[i] <- sum(q[,i])
  qs <- cbind(qs)

}

# compute Bxi
bs <- b %*% cbind(xi)

#C
C <-cbind(qs) %*% t(bs)


bbt <- b %*% t(b) -C -t(C) + sum(xi * xi)* (qs %*% t(qs))

e <- eigen(bbt, symmetric=T)

res <- list()

res$svalues <- sqrt(e$values)[1:k]
uhat=e$vectors[1:k,1:k]

res$u <- (q %*% e$vectors)[,1:k]


# debug 
# fix b 
# b <- b - qs %*% rbind(xi) 
#fixedb <- t(q) %*% fixeda
#sum(b-fixedb)
#fixedbbt=b %*% t(b)
#sum(bbt-fixedbbt) 

res$v <- (t(b- qs %*% rbind(xi) ) %*% e$vectors %*% diag(1/e$values))[,1:k]

return(res)

}






