#
A <- read.csv("/home/dmitriy/projects/github/mahout-commits/core/A.csv",
		header=F)
A <- A[order(A[,1]),]
A <- as.matrix(A[,2:ncol(A)])

U <- read.csv("/home/dmitriy/projects/github/mahout-commits/core/U.csv",
		header=F)
U <- U[order(U[,1]),]
U <- as.matrix(U[,2:ncol(U)])
U[1,1:4]
V <- read.csv("/home/dmitriy/projects/github/mahout-commits/core/V.csv",
		header=F)
V <- V[order(V[,1]),]
V <- as.matrix(V[,2:ncol(V)])


s <- svd(A)



err1 <- NULL

for (i in 1:50 ) { 
	ss <- ssvd.svd(A,k=30,p=15,qiter=1)
	errors <- abs(abs(ss$u[1,])-abs(s$u[1,1:ncol(ss$u)]))
	if (is.null(err1)) err1 <- matrix(errors,ncol =length(errors)) else 
		err1<-rbind(err1,errors)
}

colMeans(err1)

err2 <- NULL

U <- read.csv("/home/dmitriy/projects/github/mahout-commits/core/U.csv",
		header=F)
U <- U[order(U[,1]),]
U <- as.matrix(U[,2:ncol(U)])
errors <- abs(abs(U[1,])-abs(s$u[1,1:ncol(U)]))
if (is.null(err2)) err2 <- matrix(errors,ncol=length(errors)) else 
	err2<-rbind(err2,errors)

colMeans(err2)
absdiff <- colMeans(err1)-colMeans(err2)
reldiff <- absdiff/abs(s$u[1,1:length(absdiff)])

#95% confidence +/- for MR measurements
sqrt(colMeans(err2*err2)/nrow(err2))*qnorm(0.975)