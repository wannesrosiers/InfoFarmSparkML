#' @name kmeans
#' @export
setGeneric("kmeans",function(data, numericVars, responseColumn, k, eps = 0.01, maxIt = 10, logging=FALSE){standardGeneric("kmeans")})

#' @title kmeans
#' 
#' @param data
#' @param numericVars
#' @param responseColumn
#' @param k number of clusters
#' @param eps = 0.01 stopping criterium
#' @param maxIt = 10 maximum number of iterations to run
#' @return list data, scales, means
#' 
#' @export
kmeans <- function(data, numericVars, responseColumn, k, eps = 0.01, maxIt = 10, logging=FALSE){
  data   <- getColumns(data, c(numericVars, responseColumn))
  scaled <- .km_scale(data, numericVars, logging)
  data   <- scaled[["data"]]
  scales <- scaled[["scales"]]
  means  <- take(distinct(data, numericVars), k)[,numericVars]
  means  <- .orderMeans(means, numericVars)
  
  diff <- Inf
  it <- 0
  while(diff > eps){
    it <- it + 1
    prev_means <- means
    data <- .EuclideanDistance(data, numericVars, means)
    data <- .getClusters(data, k)
    means <- .getMeans(data, numericVars)
    diff <- .getDiff(prev_means, means)
    if(logging) print(paste("iteration:", it, "diff: ", diff))
    if(it >= maxIt) break
  }
  data <- removeColumns(data, eval(parse(text=paste("c('dist_", paste(1:k, collapse="', 'dist_"), "')", sep=""))))
  return(list("model"=data, "scales"=scales, "means"=means))
}

#' @name predict_kmeans
#' @export
setGeneric("predict_kmeans",function(data, scales, means, responseColumn=""){standardGeneric("predict_kmeans")})

#' @title predict_kmeans
#' 
#' @param data
#' @param scales
#' @param means
#' @param responseColumn=""
#' @return data including prediction column 'cluster'
#' 
#' @export
predict_kmeans <- function(data, scales, means, responseColumn=""){
  numericVars <- names(means)
  data <- getColumns(data, c(colnames(means), responseColumn))
  data <- .km_scale_predict(data, numericVars, scales)
  data <- .EuclideanDistance(data, colnames(means), means)
  data <- .getClusters(data, dim(means)[[1]])
  data <- removeColumns(data, eval(parse(text=paste("c('dist_", paste(1:k, collapse="', 'dist_"), "')", sep=""))))
  return(data)
}

.EuclideanDistance <- function(data, vars, means){
  for(i in 1:dim(means)[[1]]){
    for(var in vars){
      eval(parse(text=paste("data$", var, "_", i, " <- data[['", var, "']] - means[", i, ",][['", var, "']]",sep="")))
    }
    eval(parse(text=paste("data$dist_",i," <- (data[['",paste(vars, collapse= paste("_",i,"']])^2+(data[['",sep="")),"_",i,"']])^2",sep="")))
    eval(parse(text=paste("remove <- c('",paste(vars, collapse = paste("_",i,"', '",sep="")),"_", i, "')",sep="")))
    data <- removeColumns(data, remove)
  }
  return(data)
}

.getClusters <- function(data, k){
  data$cluster <- cast(data[[1]]*0, 'integer')
  for(i in 1:k){
    data$temp_cluster <- cast(data[[1]]*0, 'integer')
    for(j in 1:k){
      if(i < j){
        eval(parse(text=paste("data$temp_cluster <- data$temp_cluster + cast(data$dist_",i," <= data$dist_",j,", 'integer')", sep="")))
      }else if(j < i){
        eval(parse(text=paste("data$temp_cluster <- data$temp_cluster + cast(data$dist_",i," < data$dist_",j,", 'integer')", sep="")))
      }
    }
    eval(parse(text=paste("data$cluster <- data$cluster + cast(data$temp_cluster == ",(k-1),", 'integer') * ",i,sep="")))
    data <- removeColumns(data, c("temp_cluster"))
  }
  data <- setType(data, c("cluster"), "integer")
  return(data)
}

.getMeans <- function(data, vars){
  text <- c()
  for(var in vars){
    text <- c(text, paste("'", var, "' = avg(data$", var,")", sep = ""))
  }
  eval(parse(text=paste("means <- collect(agg(groupBy(data, 'cluster'), ", paste(text, collapse=", "),"))", sep="")))
  means <- means[,vars]
  return(.orderMeans(means, vars))
}

.orderMeans <- function(means, vars){
  eval(parse(text=paste("means <- means[order(means[", paste(1:length(vars), collapse="], means["), "]),]", sep="")))
  return(means)
}

.getDiff <- function(prev_means, means){
  diff <- (prev_means - means)^2
  return(max(diff))
}

.km_scale <- function(data, numericVars, logging=FALSE){
  if(logging) print("Scaling data")
  scales <- c()
  for(var in numericVars){
    if(logging) print(paste("...", var))
    min <- .getMin(data, var)
    max <- .getMax(data, var)
    data <- .getScaled(data, var, min, max)
    scales <- c(scales, eval(parse(text=paste("c('",var, "_min' = min, '",var,"_max' = max)", sep=""))))
  }
  if(logging) print("Done scaling data")
  return(list("data"= data, "scales"=scales))
}

.km_scale_predict <- function(data, numericVars, scales, logging=FALSE){
  if(logging) print("Scaling data")
  scales <- c()
  for(var in numericVars){
    if(logging) print(paste("...", var))
    min <- eval(parse(text=paste("scales[['",var,"_min']]", sep="")))
    max <- eval(parse(text=paste("scales[['",var,"_max']]", sep="")))
    data <- .getScaled(data, var, min, max)
  }
  if(logging) print("Done scaling data")
  return(data)
}