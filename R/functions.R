###############################
## DataFrame transformations ##
###############################
#' @name setType
#' @export
setGeneric("setType",function(data, columns, type){standardGeneric("setType")})

#' @title setType
#' 
#' @param data
#' @param columns which columns to cast
#' @param type which type to cast to
#' @return data with casted columns
#' 
#' @export

setType <- function(data, columns, type){
  if(length(columns) > 0){
    for(col in columns){
      eval(parse(text=paste("data$",col,"<-cast(data$",col,",'",type,"')", sep='')))
    }
  }
  return(data)
}

#' @name removeColumns
#' @export
setGeneric("removeColumns",function(data, columns){standardGeneric("removeColumns")})

#' @title removeColumns
#' 
#' @param data
#' @param columns
#' @return data without sprcified columns
#' 
#' @export
removeColumns <- function(data, columns){
  columns <- setdiff(names(data), columns)
  return(data[,columns])
}

#' @name getColumns
#' @export
setGeneric("getColumns",function(data, columns){standardGeneric("getColumns")})

#' @title getColumns
#' 
#' @param data
#' @param columns
#' @return data containing only specified columns
#' 
#' @export
getColumns <- function(data, columns){
  return(data[,columns])
}

.getScaled <- function(data, column, min=NULL, max=NULL){
  if(is.null(min)) min <- .getMin(data, column)
  if(is.null(max)) max <- .getMax(data, column)
  if(max != min) eval(parse(text=paste("data$",column," <- (data$", column, "-",min,")/(",max,"-",min,")", sep='')))
  return (data)
}

# @name getNumericVars
# @title getNumericVars
# 
# @param data
# @param vars=NULL
# @return vector numeric variables
# 
# @export
#setGeneric("getNumericVars",function(data, vars){standardGeneric("getNumericVars")})

.getNumericVars <- function(data, vars=NULL){
  if(is.null(vars)) vars <- names(data)
  types <- as.vector(t(as.data.frame(dtypes(data)))[,2])
  numericVars <- base::intersect(vars,names(data)[which(types=="int"|types=="double")])
  return(numericVars)
}

###############################################
## Create categories for numerical variables ##
###############################################

# @name numToCat
# @title numToCat
# @description Create categories for numerical data
# 
# @param data
# @param numericVars
# @param scales=NULL if scales are not given min and max of each column are used
# @param logging=FALSE
# @return data containing a categorical column in stead of the numerical column
# 
# @export
#setGeneric("numToCat",function(data, numericVars, scales=NULL){standardGeneric("numToCat")})

.numToCat <- function(data, numericVars, scales=NULL, logging=FALSE){
  newscale <- FALSE
  if(is.null(scales)){
    newscale <- TRUE
    scales <- c()
  }
  for(var in numericVars){
    if(logging) print(paste("Creating categories for",var))
    if(newscale){
      scales[[var]]$min <- .getMin(data, var)
      scales[[var]]$max <- .getMax(data, var)
    }
    min <- scales[[var]]$min
    max <- scales[[var]]$max
    if(max != min){
      eval(parse(text=paste("data$",var," <- (data$", var, "-",min,")/(",max,"-",min,")*100", sep='')))
      eval(parse(text=paste("data$", var, " <- (data$", var, "-data$", var, "%%1)", sep="")))
      eval(parse(text=paste("data$",var,"<- cast(data$",var,",'integer')", sep='')))
    }
  }
  if(newscale) return(list("data"=data, "scales"=scales))
  return(data)
}

####################
## Evaluate model ##
####################
#' @name confusionMatrix
#' @export
setGeneric("confusionMatrix",function(data, responseColumn, predictionColumn, logging=FALSE){standardGeneric("confusionMatrix")})

#' @title confusionMatrix
#' 
#' @param data
#' @param responseColumn
#' @param predictionColumn
#' @return confusion matrix
#' 
#' @export
confusionMatrix <- function(data, responseColumn, predictionColumn, logging=FALSE){
  k <- max(count(distinct(data[,responseColumn])), count(distinct(data[,predictionColumn])))
  if(logging) print(paste("Found", k, "dimensions for the confusionMatrix"))
  km_confusion <- collect(agg(groupBy(data, predictionColumn, responseColumn), count=n(eval(parse(text=paste("data$",predictionColumn,sep=""))))))  
  km_confusion <- km_confusion[order(km_confusion[predictionColumn], km_confusion[responseColumn]),]
  km_confusion <- matrix(km_confusion$count,nrow=k, ncol=dim(km_confusion)[[1]]/k, dimnames=list(unique(km_confusion[[predictionColumn]]),unique(km_confusion[[responseColumn]])))
  return(km_confusion)
}

#' @name getConfusionList
#' @export
setGeneric("getConfusionList",function(data, responseColumn, predictionColumn, logging=FALSE){standardGeneric("getConfusionList")})

#' @title getConfusionList
#' @description get confusion list for boolean predictionColumn
#' 
#' @param data
#' @param responseColumn column containing the response variable
#' @param predictionColumn column containing the predictions (boolean)
#' @return list containing true positives, false positives, true negatives, and false negatives
#' 
#' @export
getConfusionList <- function(data, responseColumn, predictionColumn, logging=FALSE){
  data_true  <- filter(data, data[[responseColumn]] == TRUE)
  data_false <- filter(data, data[[responseColumn]] == FALSE)
  
  result_true  <- .getTrueFalse(data_true, responseColumn, predictionColumn, count(data_true), logging=logging)  
  result_false <- .getTrueFalse(data_false, responseColumn, predictionColumn, count(data_false), logging=logging)
  
  result <- list("truePos"=result_true[[1]], "falseNeg"=result_true[[2]],
                 "falsePos"=result_false[[1]], "trueNeg"=result_false[[2]])
  return(result)
}

#' @name calculateAUC
#' @export
setGeneric("calculateAUC",function(confusionDF){standardGeneric("calculateAUC")})

#' @title calculateAUC
#' @description get area under the curve and a data.frame containing SENS and ASP (for ROC curve)
#' 
#' @param confusionDF a data.frame containing columns truePos, falsePos, trueNeg, falseNeg
#' @return list containing AUC and a data.frame containing SENS and ASP
#' 
#' @export
calculateAUC <- function(confusionDF){
  ASP  <- confusionDF$falsePos/(confusionDF$falsePos + confusionDF$trueNeg )
  SENS <- confusionDF$truePos /(confusionDF$truePos  + confusionDF$falseNeg)
  temp <- as.data.frame(cbind(ASP, SENS))
  temp <- aggregate(SENS ~ ASP, confusionDF, max)
  diff <- diff(temp$ASP)
  
  AUC <- 0
  for(i in 1:(dim(temp)[1]-1)){
    AUC <- AUC + (temp$SENS[i]+temp$SENS[i+1])/2*diff[i]
  }
  return(list(AUC, as.data.frame(temp)))
}


#' @name plotROC
#' @export
setGeneric("plotROC",function(ROCdata){standardGeneric("plotROC")})

#' @title plotROC
#' @description plot ROC-curve
#' 
#' @param ROCdata a data.frame containing SENS and ASP
#' 
#' @export
plotROC <- function(ROCdata){
  plot(ROCdata, ylim = c(0,1), xlim = c(0,1),type = 's')
  abline(coef = c(0,1))
}

#' @name getThreshold
#' @export
setGeneric("getThreshold",function(data, responseCol="response", predCol="rate", n=100, logging=FALSE){standardGeneric("getThreshold")})

#' @title getThreshold
#' @description get optimal threshold for given predictions
#' 
#' @param data
#' @param responseColumn column containing the response variable
#' @param predictionColumn column containing the predictions (double)
#' @param n=100 number of thresholds to investigate
#' @param logging=FALSE
#' @return data.frame containing true positives, false positives, true negatives, and false negatives for each investigated threshold and the optimal threshold
#' 
#' @export
getThreshold <- function(data, responseCol="response", predCol="rate", n=100, logging=FALSE){
  confusionDataFrame <- .getConfusionDataFrame(data, responseCol, predCol, n, logging)
  threshold <- .getConfusionThreshold(confusionDataFrame)
  return(list("confusionDataFrame"= confusionDataFrame, "threshold"=threshold))
}

#' @name accuracyMeasures
#' @export
setGeneric("accuracyMeasures",function(confusionDF, threshold=NULL, name="model"){standardGeneric("accuracyMeasures")})

#' @title accuracyMeasures
#' 
#' @param confusionDF a data.frame containing columns truePos, falsePos, trueNeg, falseNeg
#' @param threshold=NULL if confusionDF contains more than one row, choose a useful threshold
#' @param name="model"
#' @return data.frame containing name, accuracy, precision, recall, and f1
#' @export
accuracyMeasures <- function(confusionDF, threshold=NULL, name="model"){
  if(!is.null(threshold)){
    i <- threshold*(dim(confusionDF)[1]+1)
    confusionDF <- confusionDF[i,]
  }

  accuracy  <- (confusionDF$truePos+confusionDF$trueNeg)/(confusionDF$truePos+confusionDF$trueNeg+confusionDF$falsePos+confusionDF$falseNeg)
  precision <- confusionDF$truePos/(confusionDF$truePos+confusionDF$falsePos)
  recall    <- confusionDF$truePos/(confusionDF$truePos+confusionDF$falseNeg)
  f1        <- 2*precision*recall/(precision+recall)
  return(data.frame(model=name, accuracy=accuracy, precision=precision, recall=recall, f1=f1))
}

.getTrueFalse <- function(data, responseColumn, predictionColumn, total, threshold=NULL, logging=FALSE){
  {
  if(is.null(threshold)){
    true <- count(filter(data, data[[predictionColumn]] == TRUE))
  } 
  else{
    if(logging) print(paste("get true false for :", threshold))
    true <- count(filter(data, data[[predictionColumn]] > threshold))
  }  
  }
  false     <- total - true
  return(c(true,false))
}


# @name getConfusionDataFrame
# @title getConfusionDataFrame
# 
# @param data
# @param responseColumn column containing the response variable
# @param predictionColumn column containing the predictions (double)
# @param n=100 number of thresholds investigated
# @param logging=FALSE
# @return data.frame containing true positives, false positives, true negatives, and false negatives for each investigated threshold
# 
# @export
#setGeneric("getConfusionDataFrame",function(data, responseColumn, predictionColumn, n=100){standardGeneric("getConfusionDataFrame")})

# @title getConfusionDataFrame
# 
# @param data
# @param responseColumn column containing the response variable
# @param predictionColumn column containing the predictions (double)
# @param n=100 number of thresholds investigated
# @param logging=FALSE
# @return data.frame containing true positives, false positives, true negatives, and false negatives for each investigated threshold
# 
# @export
.getConfusionDataFrame <- function(data, responseColumn, predictionColumn, n=100, logging=FALSE){
  data_true <- filter(data, data[[responseColumn]] == TRUE)
  data_false <- filter(data, data[[responseColumn]] == FALSE)
  cache(data_true)
  cache(data_false)
  
  count_true <- count(data_true)
  count_false <- count(data_false)
  
  if(logging) print("get truePos and falseNeg")
  thresh <- (0:n)/n
  result_true <- lapply(thresh, function(threshold){.getTrueFalse(data_true, responseColumn, predictionColumn, count_true, threshold, logging)})
  result_true <- t(as.data.frame(result_true))
  colnames(result_true) <- c("truePos", "falseNeg")
  rownames(result_true) <- NULL
  
  if(logging) print("get falsePos and trueNeg")
  result_false <- lapply(thresh, function(threshold){.getTrueFalse(data_false, responseColumn, predictionColumn, count_false, threshold, logging)})
  result_false <- t(as.data.frame(result_false))
  colnames(result_false) <- c("falsePos", "trueNeg")
  rownames(result_false) <- NULL
  
  result <- as.data.frame(cbind(result_true, result_false))
  return(result)
}

.getConfusionThreshold <- function(confusionDF){
  n <- dim(confusionDF)[1]-1
  confusionDF$precision <- confusionDF$truePos / (confusionDF$truePos + confusionDF$falsePos)
  confusionDF$recall    <- confusionDF$truePos / (confusionDF$truePos + confusionDF$falseNeg)
  confusionDF$f1        <- 2 * confusionDF$precision * confusionDF$recall / (confusionDF$precision + confusionDF$recall)
  
  return(which.max(confusionDF$f1)/n)
}


.getMin <- function(data, column){
  return(first(orderBy(data, column))[[column]])
}

.getMax <- function(data, column){
  return(first(arrange(data, desc(data[[column]])))[[column]])
}