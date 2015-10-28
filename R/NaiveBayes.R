#' @name predict_NaiveBayes
#' @export
setGeneric("predict_NaiveBayes",function(data, scales, model, logging=FALSE){standardGeneric("predict_NaiveBayes")})

#' @title predict_NaiveBayes
#' 
#' @param data
#' @param scales Scales returned by NaiveBayes(data, vars, ...)
#' @param model Model returned by NaiveBayes(data, vars, ...)
#' @param logging=FALSE enable/disable logging
#' @export
predict_NaiveBayes <- function(data, scales, model, logging=FALSE){
  numericVars <- .getNumericVars(data, names(model))
  numpart <- SparkR:::numPartitions(SparkR:::toRDD(data))
  if(logging) print("Creating categorical columns")
  data <- .numToCat(data, numericVars, scales)
  data <- repartition(data, numpart)
  
  if(logging) print("Starting prediction...")
  for(var in names(model)){
    if(logging) print(paste("...predicting",var))
    data <- join(data, model[[var]], data[[var]]==model[[var]]$category)#, joinType="left_outer")
    data$category <- NULL
    {
      if("prev_rate" %in% names(data)) eval(parse(text=paste("data$prev_rate <-data$prev_rate*data$rate")))
      else data <- withColumnRenamed(data, "rate", "prev_rate")
    }
    data$rate <- NULL
  }
  data <- withColumnRenamed(data, "prev_rate", "rate")
  if(logging) print("... scaling prediction")
data <- .getScaled(data, "rate")
if(logging) print("Done predicting")
return(data)
}

#' @name NaiveBayes
#' @export
setGeneric("NaiveBayes",function(data, vars, nrOfNumericCategories = 100){standardGeneric("NaiveBayes")})

#' @title NaiveBayes
#' 
#' @param data
#' @param vars Variables to create model
#' @param nrOfNumericCategories=100
#' @export
NaiveBayes <- function(data, vars, nrOfNumericCategories = 100){
  types <- as.vector(t(as.data.frame(dtypes(data)))[,2])
  numericVars <- base::intersect(vars,names(data)[which(types=="int"|types=="double")])
  
  numpart   <- SparkR:::numPartitions(SparkR:::toRDD(data))
  modeldata <- .numToCat(data, numericVars)
  scales <- modeldata[["scales"]]
  model <- .NaiveBayesModel(modeldata[["data"]], vars, nrOfNumericCategories)
  return(list("model"=model, "scales"=scales))
}

.NaiveBayesModel <- function(data, vars, nrOfNumericCategories){
  print("Creating model...")
  data <- setType(data, c("response"), "integer")
  registerTempTable(data, "temptable")
  result <- lapply(vars, function(var){.getModel("temptable", var, nrOfNumericCategories)})
  dropTempTable(sqlContext, "temptable")
  
  names(result) <- vars
  print("... model created")
  return(result)
}

.getModel <- function(tablename, var, nrOfNumericCategories){
  print(paste("Get model for", var))
  query <- paste("SELECT ", var, " as category, (sum(response)/count(response)) as rate FROM ", tablename, " GROUP BY ", var ,sep="")
  model <- sql(sqlContext, query)
  repartition(model, config$numberOfPartitions)
  if(var %in% config$numericVars && count(model) < (nrOfNumericCategories+1)){
    model <- .completeNumericModel(model, nrOfNumericCategories)
    model <- createDataFrame(sqlContext,model)
  }
  cache(model)
  return(model)
}

.completeNumericModel <- function(model, n){
  local_model <- collect(model)
  for( i in 0:n){ if(! i %in% local_model$category) local_model <- rbind(local_model, c(i, -1)) }
  local_model <- local_model[with(local_model, order(local_model$category)), ]
  row.names(local_model) <- NULL
  
  # make sure first element has rate
  if(local_model[1,2]==-1){
    i <- 1
    while(local_model[i,2]==-1) i <- i+1
    local_model[1,2] <- local_model[i,2]
  }
  
  # make sure last element has rate
  if(local_model[(n+1),2]==-1){
    i <- n
    while(local_model[i,2]==-1) i <- i-1
    local_model[(n+1),2] <- local_model[i,2]
  }
  
  # fill in the gaps
  local_model <- .fillNumericModelGaps(local_model)
  return(local_model)
}

.fillNumericModelGaps <- function(local_model){
  index_unknown <- which(local_model$rate == -1)
  i <- 1
  while(i <= length(index_unknown)){
    current_index <- index_unknown[i]
    index <- current_index
    j <- i
    while((index+1) %in% index_unknown){
      j <- j + 1
      index <- index + 1
    }
    prev_rate <- local_model[current_index-1,2]
    next_rate <- local_model[index+1,2]
    step <- (next_rate - prev_rate)/(index+1 - current_index-1)
    for(k in current_index:index){
      local_model[k,2] <- prev_rate+(k-current_index+1)*(next_rate-prev_rate)/(index-current_index+2)
    }
    i <- j + 1
  }
  return(local_model)
}