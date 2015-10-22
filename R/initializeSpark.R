#' @name initializeSpark
#' @export
setGeneric("initializeSpark",function(sparkDirectory,jarPath,size){standardGeneric("initializeSpark")})

#' @title initializeSpark
#' 
#' @param sparkDirectory
#' @param size spark executor memory
#' @return list(sc, sqlContext) sparkContext and sqlContext
#' 
#' @export
initializeSpark <- function(sparkDirectory,size){
  Sys.setenv('SPARKR_SUBMIT_ARGS'='"--packages" "com.databricks:spark-csv_2.10:1.0.3" "sparkr-shell"')
  
  # Set SPARK_HOME
  Sys.setenv(SPARK_HOME=sparkDirectory)
  
  # Load SparkR from the installed directory
  .libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
  library(SparkR)
  
  # Initialize a sparkContext and sqlContext
  sc <- sparkR.init(sparkEnvir=list(spark.executor.memory=size))
  sqlContext <- sparkRSQL.init(sc)
  
  # Return the sparkContext and sqlContext
  return(list(sc, sqlContext))
}