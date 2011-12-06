n<-100
m<-200
k<-10

qi<-0

#simulated input
svalsim<-diag(k:1)

usim<- qr.Q(qr(matrix(rnorm(m*k), nrow=m,ncol=k)))
vsim<- qr.Q(qr( matrix(rnorm(n*k), nrow=n,ncol=k)))

xisim=1:n

x<- usim %*% svalsim %*% t(vsim) 
for (i in 1:m) x[i,]<- x[i,]+xisim

# SVD test
res <- ssvd.svd(x,k, qiter=1 );

res$svalues

svdControl<- svd(x,nu=k,nv=k)

svdControl$d[1:k]


## PCI test 
# compute median xi
xi<-rep(0,n)
for (i in 1:m ) xi <- xi + x[i,]
xi <- xi / m

xfixed=matrix(nrow=m,ncol=n)
for ( i in 1:m) xfixed[i,]=x[i,]-xi

	respci=ssvd.cpci(x,k,qiter=0)
	ressvd=ssvd.svd(xfixed,k,qiter=1)
	
	respci$svalues
	ressvd$svalues
