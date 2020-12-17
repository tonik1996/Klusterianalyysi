package assignment20

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{window, column, desc, col}


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, IntegerType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg}

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}




import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansSummary}


import java.io.{PrintWriter, File}


//import java.lang.Thread
import sys.process._


import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.immutable.Range

object assignment  {
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)
                       
  
  val spark = SparkSession.builder()
                          .appName("DIP20")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate() 
                          
  val dataK5D2 =  spark.read
                       .format("csv")
                       .option("delimiter", ",")
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .csv("data/dataK5D2.csv")

  val dataK5D3 =  spark.read
                       .format("csv")
                       .option("delimiter", ",")
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .csv("data/dataK5D3.csv")

  // Map the LABEL column of dataK5D3 to a numeric scale 0-1 and create
  // a new DataFrame with a column num(LABEL).
  import org.apache.spark.ml.feature.StringIndexer
  val indexer = new StringIndexer()
    .setInputCol("LABEL")
    .setOutputCol("num(LABEL)")
  val dataK5D3WithLabels = indexer.fit(dataK5D3).transform(dataK5D3)
  
  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {

    // Create a VectorAssembler for mapping input columns "a" and "b" to "rawfeatures" 
    // vector column.
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("a", "b"))
      .setOutputCol("rawfeatures")
      
    // Apply the VectorAssembler for the DataFrame df by pipelining.
    val transformationPipeline = new Pipeline()
      .setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(df)
    val transformedTraining = pipeLine.transform(df)

    // Normalize the data to improve clustering results.
    import org.apache.spark.ml.feature.Normalizer
    val normalizer = new Normalizer()
      .setInputCol("rawfeatures")
      .setOutputCol("features")
    val normTraining = normalizer.transform(transformedTraining)
    
    // Train a k-means model with k clusters. As k-means clustering algorithm starts 
    // with k randomly selected centroids, the setSeed function is applied in order 
    // to set a seed for the random number generator and to make the results reproducible.
    val kmeans = new KMeans()
                    .setK(k)
                    .setSeed(1L)
                    
    // Fit the k-means model to the transformed and normalised training data.
    val kmModel = kmeans.fit(normTraining)

    // Obtain the coordinates of the centroids of the clusters and map them into an 
    // array of tuples.
    val centers = kmModel.clusterCenters.map(line => (line(0),line(1))).toArray
    
    return centers
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {

    // Create a VectorAssembler for mapping input columns "a", "b" and "c" to "rawfeatures" 
    // vector column.
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("a", "b", "c"))
      .setOutputCol("rawfeatures")
    
    // Apply the VectorAssembler for the DataFrame df by pipelining.
    val transformationPipeline = new Pipeline()
      .setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(df)
    val transformedTraining = pipeLine.transform(df)

    // Normalize the data to improve clustering results.
    import org.apache.spark.ml.feature.Normalizer
    val normalizer = new Normalizer()
      .setInputCol("rawfeatures")
      .setOutputCol("features")
    val normTraining = normalizer.transform(transformedTraining)
    
    // Train a k-means model with k clusters. As k-means clustering algorithm starts 
    // with k randomly selected centroids, the setSeed function is applied in order to 
    // set a seed for the random number generator and to make the results reproducible.
    val kmeans = new KMeans()
                    .setK(k)
                    .setSeed(1L)
                    
    // Fit the k-means model to the transformed and normalised training data.
    val kmModel = kmeans.fit(normTraining)

    // Obtain the coordinates of the centroids of the clusters and map them into an array 
    // of tuples.
    val centers = kmModel.clusterCenters.map(line => (line(0),line(1),line(2))).toArray

    return centers
  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    
    // Create a VectorAssembler for mapping input columns "a", "b" and "num(LABEL)" 
    // to "rawfeatures" vector column.
    val vectorAssembler = new VectorAssembler()
        .setInputCols(Array("a", "b", "num(LABEL)"))
        .setOutputCol("rawfeatures")
      
    // Apply the VectorAssembler for the DataFrame df by pipelining.
    val transformationPipeline = new Pipeline()
      .setStages(Array(vectorAssembler))
    val transformedTraining = transformationPipeline.fit(df).transform(df)

    // Normalize the data to improve clustering results.
    import org.apache.spark.ml.feature.Normalizer
    val normalizer = new Normalizer()
      .setInputCol("rawfeatures")
      .setOutputCol("features")
    val normTraining = normalizer.transform(transformedTraining)
    
    // Train a k-means model with k clusters. As k-means clustering algorithm starts 
    // with k randomly selected centroids, the setSeed function is applied in order to 
    // set a seed for the random number generator and to make the results reproducible.
    val kmeans = new KMeans()
                    .setK(5)
                    .setSeed(1L)
                    
    // Fit the k-means model to the transformed training data.
    val kmModel = kmeans.fit(normTraining)

    // Obtain the two-dimensional coordinates of the centroids of those clusters 
    // which have a big possibility for the Fatal condition (i.e. the value of the 
    // third dimension of the cluster mean is not close to 0). Moreover, map them 
    // into an array of tuples.
    val centers = kmModel.clusterCenters.filter(line => line(2) >= 0.1) 
    .map(line => (line(0),line(1))).toArray
    
    return centers
  }

  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    // Create a VectorAssembler for mapping input columns "a" and "b" to "rawfeatures" 
    // vector column.
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("a", "b"))
      .setOutputCol("rawfeatures")
      
    // Apply the VectorAssembler for the DataFrame df by pipelining.
    val transformationPipeline = new Pipeline()
      .setStages(Array(vectorAssembler))
    val pipeLine = transformationPipeline.fit(df)
    val transformedTraining = pipeLine.transform(df)

    // Normalize the data to improve clustering results.
    import org.apache.spark.ml.feature.Normalizer
    val normalizer = new Normalizer()
      .setInputCol("rawfeatures")
      .setOutputCol("features")
    val normTraining = normalizer.transform(transformedTraining)
    
    // Initialize the arrays for the Elbow method; k contains the number of clusters
    // in range low and high, where low is the lowest number of clusters and high the
    // highest number of clusters. The costs for each clustering will be stored to the
    // array costs.
    import Array._
    var k = range(low,high+1)
    var costs = new Array[Double](0)
   
    // Iterate over the number of clusters, fit the k-means model as well as compute 
    // the cost for each k value and append the cost value to the array.
    for (i <- k) {
      val kmeans = new KMeans()
                       .setK(i)
                       .setSeed(1L)
      val kmModel = kmeans.fit(normTraining)                 
      val cost = kmModel.computeCost(normTraining)
      costs :+= cost
    }

    // Merge the two arrays into one array of tuples.
    val elbow = k.zip(costs)
    
    return elbow
  }
}
