#
A <- read.csv("/home/dmitriy/projects/github/mahout-commits/core/A.csv",
		header=F)
A <- A[order(A[,1]),]
A <- as.matrix(A[,2:ncol(A)])

U <- read.csv("/home/dmitriy/projects/github/mahout-commits/core/U.csv",
		header=F)
U <- U[order(U[,1]),]
U <- as.matrix(U[,2:ncol(U)])
V <- read.csv("/home/dmitriy/projects/github/mahout-commits/core/V.csv",
		header=F)
V <- V[order(V[,1]),]
V <- as.matrix(V[,2:ncol(V)])

s <- svd(A)

	ss <- ssvd.svd(A,k=30,p=15,qiter=2)
	ss$u[1,1:4]