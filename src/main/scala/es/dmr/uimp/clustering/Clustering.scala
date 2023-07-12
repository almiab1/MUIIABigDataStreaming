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

    val featureDf = df.groupBy("InvoiceNo").agg(
      avg("UnitPrice").as("AvgUnitPrice"),
      min("UnitPrice").as("MinUnitPrice"),
      max("UnitPrice").as("MaxUnitPrice"),
      first("Hour").as("Time"),
      sum("Quantity").as("NumberItems")// ,
      // first("CustomerID").as("CustomerID"),
      // last("InvoiceDate").as("InvoiceDate")
      )

    println("#################### New Featurized data ####################")
    featureDf.show(5)
    println("#################### END FEATURIZE DATA ####################")

    featureDf
  }

  def filterData(df : DataFrame) : DataFrame = {
   // TODO: Filter cancelations and invalid

    println("#################### INIT FILTER DATA ####################")

    val filterDf = df.filter(
      !col("InvoiceNo").startsWith("C") &&
      // col("CustomerID").isNotNull &&
      // col("InvoiceDate").isNotNull &&
      col("Time").isNotNull &&
      col("NumberItems") > 0 &&
      col("AvgUnitPrice") > 0 &&
      col("MinUnitPrice") > 0 &&
      col("MaxUnitPrice") > 0 &&
      col("Time") >= 0// &&
      // col("InvoiceDate").notEqual("")
    )

    println("#################### Filtered data ####################")
    filterDf.show(5)
    println("######################## END data #####################")

    filterDf
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
    println("\n#################### ELBOW SELECTION ####################\n")
    for (i <- 1 until costs.length) {
      val err_ratio = costs(i) / costs(i - 1)
      println(s"i=$i \n costs(i) / costs(i - 1): ${costs(i)} / ${costs(i-1)} = $err_ratio \n Selected K: ${i - 1}")
      if (err_ratio > ratio) {
        println(s"$i greater than $ratio --> Selected K: ${i - 1}")
        println("\n#################### END ELBOW SELECTION ####################\n")
        return i - 1
      }
    }
    return costs.length - 1 // return the maximum k if no elbow point is found
  }

  def saveThreshold(threshold : Double, fileName : String) = {
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    // decide threshold for anomalies
    bw.write(threshold.toString) // last item is the threshold
    bw.close()
  }

}