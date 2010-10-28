library('skmeans')
x <- readCM('affinities.cm')
mat <- skmeans(x, 50, 'lihc')
mat
