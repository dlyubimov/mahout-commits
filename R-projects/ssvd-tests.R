n<-1000
m<-2000
k<-10

qi<-0

#simulated input
svalsim<-diag(k:1)

usim<- qr.Q(qr(matrix(rnorm(m*k), nrow=m,ncol=k)))
vsim<- qr.Q(qr( matrix(rnorm(n*k), nrow=n,ncol=k)))
xisim=1:n
x<- usim %*% svalsim %*% t(vsim) + xisim

res <- ssvd.svd(x,k, qiter=1 );

res$svalues

svdControl<- svd(x,nu=k,nv=k)

svdControl$d[1:k]
