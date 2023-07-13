package es.dmr.uimp.realtime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import es.dmr.uimp.realtime.PipelineFunctions._

import java.util.HashMap
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object InvoicePipeline {

  case class Purchase(invoiceNo: String, quantity: Int, invoiceDate: String,
                      unitPrice: Double, customerID: String, country: String)

  case class Invoice(invoiceNo: String, avgUnitPrice: Double,
                     minUnitPrice: Double, maxUnitPrice: Double, time: Double,
                     numberItems: Double, lastUpdated: Long, lines: Int, customerId: String, state: Int)

  // State to indicate estate of invoice
  object InvoiceStatus {
    val NonEmitted = 0
    val Emitting = 1
    val Emitted = 2
  }

  def main(args: Array[String]) {

    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zkQuorum, group, topics, numThreads, brokers) = args
    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))
    ssc.sparkContext.setLogLevel("WARN")

    // Checkpointing
    ssc.checkpoint("./checkpoint")

    // TODO: Load model and broadcast

    val (kmeansModel, kmeansThreshold) = loadKMeansAndThreshold(sc, modelFile, thresholdFile)
    val (bisectModel, bisectThreshold) = loadBisectKMeansAndThreshold(sc, modelFileBisect, thresholdFileBisect)

    val bcKmeansThreshold = sc.broadcast(kmeansThreshold)
    val bcBisectThreshold = sc.broadcast(bisectThreshold)

    // Broadcast the Kafka brokers to make them available in all nodes
    val broadcastBrokers = ssc.sparkContext.broadcast(brokers)

    // TODO: Build pipeline

    // Connect to Kafka and parse input data
    val purchasesFeed = connectToPurchases(ssc, zkQuorum, group, topics, numThreads)

    // Parse the input data and create the purchases DStream
    val purchasesDStream = parsePurchases(purchasesFeed)
    // purchasesDStream.print(5)

    // Create a DStream of invoices and update the state
    val invoicesDStream = purchasesDStream.updateStateByKey(updateInvoice)
    invoicesDStream.print(5)

    // TODO: rest of pipeline

    val invalidDStream = invalidPipeline(invoicesDStream) // Get invalid invoices
    invalidDStream.foreachRDD(rdd => publishToKafka("invalid_invoices")(broadcastBrokers)(rdd))

    // Define window and slide interval
    val WINDOW_LENGTH = 8 // 8 minutes
    val SLIDE_INTERVAL = 60 // 1 minute
    val cancelDStream = cancellationPipeline(invoicesDStream, WINDOW_LENGTH, SLIDE_INTERVAL) // Get cancelations in the last 8 minutes every 1 minute
    cancelDStream.foreachRDD(rdd => publishToKafka("cancelations_ma")(broadcastBrokers)(rdd))

    // TODO: Find anomalies using KMeans
    val anomaliesKmeans = clusteringPipeline(invoicesDStream, Left(kmeansModel), bcKmeansThreshold)
    anomaliesKmeans.print(5)
    anomaliesKmeans.foreachRDD(rdd => publishToKafka("anomalies_kmeans")(broadcastBrokers)(rdd))
    // TODO: Find anomalies using Bisecting KMeans
    // anomaliesBisection.foreachRDD(rdd => publishToKafka("anomalies_kmeans_bisect")(broadcastBrokers)(rdd))

    ssc.start() // Start the computation
    ssc.awaitTermination()
  }


  // ------------------- LOAD MODELS AND THRESHOLDS -------------------
  def loadKMeansAndThreshold(sc: SparkContext, modelFile: String, thresholdFile: String): Tuple2[KMeansModel, Double] = {
    val kmeans = KMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map { line => line.toDouble }.first()

    (kmeans, threshold)
  }

  def loadBisectKMeansAndThreshold(sc: SparkContext, modelFile: String, thresholdFile: String): Tuple2[BisectingKMeansModel, Double] = {
    val bisectModel = BisectingKMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map { line => line.toDouble }.first()

    (bisectModel, threshold)
  }

  // ------------------- KAFKA METHODS -------------------

  // Kafka configuration
  def kafkaConf(brokers: String) = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  // Publish the anomalies to Kafka topic
  def publishToKafka(topic: String)(kafkaBrokers: Broadcast[String])(rdd: RDD[(String, String)]) = {
    rdd.foreachPartition(partition => {
      val producer = new KafkaProducer[String, String](kafkaConf(kafkaBrokers.value))
      partition.foreach(record => {
        producer.send(new ProducerRecord[String, String](topic, record._1, record._2.toString))
      })
      producer.close()
    })
  }

  // Connect to Kafka and parse input data
  def connectToPurchases(ssc: StreamingContext, zkQuorum: String, group: String,
                         topics: String, numThreads: String): DStream[(String, String)] = {

    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
  }

  // ------------------- PARSE FUNCTIONS -------------------
  def parsePurchases(purchasesFeed: DStream[(String, String)]): DStream[(String, Purchase)] = {
    purchasesFeed.map { case (_, line) =>
      val settings = new CsvParserSettings()
      val csvParser = new CsvParser(settings)
      val parsedLine = csvParser.parseLine(line)

      val invoiceNo = parsedLine(0)
      val quantity = parsedLine(3).toInt
      val invoiceDate = parsedLine(4)
      val unitPrice = parsedLine(5).toDouble
      val customerID = parsedLine(6)
      val country = parsedLine(7)
      (invoiceNo, Purchase(invoiceNo, quantity, invoiceDate, unitPrice, customerID, country))
    }
  }
}
