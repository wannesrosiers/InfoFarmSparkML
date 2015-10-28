#' @name recommenderModel
#' @title recommenderModel
#' 
#' @param data
#' @param key columnName for keys
#' @param value columnName for values
#' @param supportThreshold
#' @return DataFrame containing model
#' 
#' @export
setGeneric("recommenderModel",function(data, key, value, supportThreshold, logging=FALSE){standardGeneric("recommenderModel")})

#' @title recommenderModel
#' 
#' @param data
#' @param key columnName for keys
#' @param value columnName for values
#' @param supportThreshold
#' @return DataFrame containing model
#' 
#' @export
recommenderModel <- function(data, key, value, supportThreshold, logging=FALSE){
  cache(data)
  numpart <- SparkR:::numPartitions(SparkR:::toRDD(data))
  ## FILTER FOLLOWED_USER WITH ENOUGH OCCURENCES
  if(logging) print("FILTER VALUE WITH ENOUGH OCCURENCES...")
  data    <- .singleSupport(data, supportThreshold, value, key, logging)
  if(logging){
    singleCount <- count(data)
    print(paste("Number of single useful values:",singleCount))
  }
  
  ## JOIN DATA
  if(logging) print("JOIN DATA...")
  data <- .coupleValues(data, key, value)
  if(logging){
    pairCount <- count(data)
    print(paste("Number of value couples:",pairCount))
  }
  
  ## GROUP JOINED DATA
  if(logging) print("GROUP JOINED DATA...")
  data <- .calculateRecommendations(data, key, value, supportThreshold)
  if(logging){
    usefullCount <- count(data)
    print(paste("Number of useful value couples:",usefullCount))
  }
  
  ## ADD CONFIDENCE AND LIFT COLUMNS
  if(logging) print("ADD CONFIDENCE AND LIFT COLUMNS...")
  data <- .getResults(data)
  if(logging){
    ruleCount <- count(data)
    print(paste("Number of rules calculated:",ruleCount))
  }
  return(data)
}

#' @name recommenderModel
#' @title recommenderModel
#' 
#' @param data
#' @param model
#' @param key element for which recommendations should be created
#' @param keyColumn columnName for keys
#' @param valueColumn columnName for values
#' @param type 'confidence' or 'lift'
#' @param threshold
#' @param limit return top limit recommendations
#' @param logging=FALSE
#' @return DataFrame containing recommendations
#' 
#' @export
setGeneric("getRecommendations",function(data, model, key, keyColumn, valueColumn, type, threshold=NULL, limit=NULL, logging=FALSE)
  {standardGeneric("getRecommendations")})

#' @title getRecommendations
#' 
#' @param data
#' @param model
#' @param key element for which recommendations should be created
#' @param keyColumn columnName for keys
#' @param valueColumn columnName for values
#' @param type 'confidence' or 'lift'
#' @param threshold
#' @param limit return top limit recommendations
#' @param logging=FALSE
#' @return DataFrame containing recommendations
#' 
#' @export
getRecommendations <- function(data, model, key, keyColumn, valueColumn, type, threshold=NULL, limit=NULL, logging=FALSE){
  values <- unlist(collect(filter(data, data[[keyColumn]]==key)[,valueColumn]), use.names = FALSE)
  if(logging) print(paste("Found", length(values), "values for", key))
  result <- .getRecommendationsForValue(model, valueColumn, values, type, threshold, limit, logging)
  return(result)
}

# @name singleSupport
# @title singleSupport
# 
# @param data
# @param model
# @param supportThreshold
# @param groupbyColumn column containing values that need to occur at least supportThreshold times
# @param countColumn another column in the DataFrame
# @param logging=FALSE
# @return DataFrame containing only those rows that satisfy the threshold
# 
# @export
#setGeneric("singleSupport",function(data, supportThreshold, groupbyColumn, countColumn, logging=FALSE)
#{standardGeneric("singleSupport")})

# @title singleSupport
# 
# @param data
# @param model
# @param supportThreshold
# @param groupbyColumn column containing values that need to occur at least supportThreshold times
# @param countColumn another column in the DataFrame
# @param logging=FALSE
# @return DataFrame containing only those rows that satisfy the threshold
# 
# @export
.singleSupport <- function(data, supportThreshold, groupbyColumn, countColumn, logging=FALSE){
  groupedData <- agg(groupBy(data, groupbyColumn), count = n(data[[countColumn]]))
  if(logging) print(paste("count", groupbyColumn,"before processing:", count(groupedData)))
  groupedData <- filter(groupedData, groupedData$count>supportThreshold)
  cache(groupedData)
  if(logging) print(paste("count", groupbyColumn,"after processing:", count(groupedData)))
  groupedData <- withColumnRenamed(groupedData, groupbyColumn, paste(groupbyColumn,"2",sep=""))
  
  data <- join(data, groupedData, data[[groupbyColumn]] == groupedData[[paste(groupbyColumn,"2",sep="")]])
  data <- removeColumns(data, paste(groupbyColumn, "2", sep=""))
  data <- filter(data, data$count > supportThreshold)
  cache(data)
  return(data)
}

.getRecommendationsForValue <- function(model, filterColumn, values, type, threshold = NULL, limit = NULL, logging=FALSE){
  if(!type %in% c("confidence", "lift")) return(NULL)
  model   <- arrange(model, desc(model[[type]]))
  if(!is.null(threshold))  model   <- filter(model, model[[type]] > threshold)
  
  query <- paste("SELECT ", filterColumn, ", recommendation, ", type, " FROM model WHERE ",
                 filterColumn, " IN (", paste(values, collapse=", "),")", sep="")
  if(!is.null(limit)) query <- paste(query, "LIMIT", limit)
  
  registerTempTable(model, "model")
  result <- sql(sqlContext, query)
  dropTempTable(sqlContext, "model")
  count <- count(result)
  if(logging) print(paste("Found",count,"recommendation(s)"))
  if(count==0) return(NULL)
  return(result)
}

.getResults <- function(data, count="count", count2="count2", inters="inters", logging=FALSE){
  data$confidence <- data[[inters]]/data[[count]]
  data$lift <- data[[inters]]/(data[[count]]*data[[count2]])
  data <- removeColumns(data, c(count, count2, inters))
  return(data)
}

.coupleValues <- function(data, key, value, count="count", numberOfPartitions=4){
  temp <- data
  temp <- withColumnRenamed(temp, key, paste(key,"2", sep=""))
  temp <- withColumnRenamed(temp, value, paste(value,"2", sep=""))
  temp <- withColumnRenamed(temp, count, paste(count,"2", sep=""))
  data <- join(data, temp, data[[key]] == temp[[paste(key,"2", sep="")]])
  data <- removeColumns(data, c(paste(key,"2", sep="")))
  data <- filter(data, data[[value]] != data[[paste(value,"2", sep="")]])
  return(data)
}

.calculateRecommendations <- function(data, key, value, supportThreshold, count="count", numpart=2){
  data <- agg(groupBy(data, value, count, paste(value,"2", sep=""), paste(count,"2", sep="")), inters=n(data[[key]]))
  data <- repartition(data, numpart)
  data <- filter(data, data$inters > supportThreshold)
  data <- withColumnRenamed(data, paste(value,"2", sep=""), "recommendation")
  return(data)
}