package es.dmr.uimp.clustering

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by root on 3/12/17.
  */
object Clustering {
  /**
    * Load data from file, parse the data and normalize the data.
    */
  def loadData(sc: SparkContext, file : String) : DataFrame = {
    val sqlContext = new SQLContext(sc)

    // Function to extract the hour from the date string
    val gethour =  udf[Double, String]((date : String) => {
      var out = -1.0
      if (!StringUtils.isEmpty(date)) {
        val hour = date.substring(10).split(":")(0)
        if (!StringUtils.isEmpty(hour))
          out = hour.trim.toDouble
      }
      out
    })

    // Load the csv data
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(file)
      .withColumn("Hour", gethour(col("InvoiceDate")))

    df
  }

  def featurizeData(df : DataFrame) : DataFrame = {
    // TODO : Featurize the data
    // Group by InvoiceNo and compute the average, min and max of the UnitPrice
    println("#################### INIT FEATURIZE DATA ####################")

    println("#################### Original data ####################")
    df.show(5)

    var featureDf = df.groupBy("InvoiceNo")
      .agg(avg("UnitPrice").as("AvgUnitPrice"),
        min("UnitPrice").as("MinUnitPrice"),
        max("UnitPrice").as("MaxUnitPrice"),
        count("Quantity").as("NumberItems"),
        first("Hour").as("Time"),
        first("CustomerID").as("CustomerID"),
        last("InvoiceDate").as("InvoiceDate")
      ).orderBy("InvoiceNo")

    println("#################### New Featurized data ####################")
    featureDf.show(5)

    println("#################### END FEATURIZE DATA ####################")

    featureDf
    // df
  }

  def filterData(df : DataFrame) : DataFrame = {
   // TODO: Filter cancelations and invalid

    println("#################### INIT FILTER DATA ####################")

    var filterDf = df.filter(!col("InvoiceNo").startsWith("C") ||
                        col("InvoiceNo").isNotNull ||
                        col("CustomerID").isNotNull ||
                        col("CustomerID").notEqual("") ||
                        col("InvoiceDate").isNotNull ||
                        col("InvoiceDate").notEqual(""))

    println("#################### Filtered data ####################")
    filterDf.show(5)
    println("######################## END data #####################")

    filterDf
    // df
  }

  def toDataset(df: DataFrame): RDD[Vector] = {
    val data = df.select("AvgUnitPrice", "MinUnitPrice", "MaxUnitPrice", "Time", "NumberItems").rdd
      .map(row =>{
        val buffer = ArrayBuffer[Double]()
        buffer.append(row.getAs("AvgUnitPrice"))
        buffer.append(row.getAs("MinUnitPrice"))
        buffer.append(row.getAs("MaxUnitPrice"))
        buffer.append(row.getAs("Time"))
        buffer.append(row.getLong(4).toDouble)
        val vector = Vectors.dense(buffer.toArray)
        vector
      })

    data
  }

  def elbowSelection(costs: Seq[Double], ratio : Double): Int = {
    // TODO: Select the best model
    val ratios = ArrayBuffer[Double]()

    for (k <- 1 until costs.length) {
      val currentRatio = costs(k) / costs(k - 1)
      ratios += currentRatio
    }

    val selectedK = ratios.indexWhere(_ > ratio) + 1

    // Print the results
    println()
    println()
    println("#################### ELBOW SELECTION ####################")
    println("Costs: " + costs)
    println("Ratios: " + ratios)
    println("Selected K: " + selectedK)
    println("#################### END ELBOW SELECTION ####################")
    println()
    println()

    selectedK
  }

  def saveThreshold(threshold : Double, fileName : String) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    // decide threshold for anomalies
    bw.write(threshold.toString) // last item is the threshold
    bw.close()
  }

}
