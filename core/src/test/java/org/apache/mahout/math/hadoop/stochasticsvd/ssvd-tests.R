n<-100
m<-200
k<-10

qi<-1

#simulated input
svalsim<-diag(c(k:1,rep(0.1,n-k)))

usim<- qr.Q(qr(matrix(rnorm(m*n, mean=3), nrow=m,ncol=n)))
vsim<- qr.Q(qr( matrix(rnorm(n*n,mean=5), nrow=n,ncol=n)))

xisim=(1:n) 

x<- usim %*% svalsim %*% t(vsim) 
for (i in 1:m) x[i,]<- x[i,]+xisim


xi <- colMeans(x)

# SVD test: compare k singular values out of ssvd.svd and regular svd
res <- ssvd.svd1(x,k, qiter=0 );

res$svalues

svdControl<- svd(x,nu=k,nv=k)

svdControl$d[1:k]


## PCActest 
# compute median xi

xfixed=matrix(nrow=m,ncol=n)
for ( i in 1:m) xfixed[i,]=x[i,]-xi


	respca=ssvd.cpca(x,k,qiter=qi)
	# compare also with results when Y fix is ignored
	respca1=ssvd.cpca(x,k,qiter=qi,fixY=F)
	
	ressvd=ssvd.svd(xfixed,k,qiter=qi)
	
	# compare 3 sets of singular values
	respca$svalues
	respca1$svalues
	ressvd$svalues
	
	#compare first rows of singular vectors 
	respca$v[1,]
	respca1$v[1,]
	ressvd$v[1,]
