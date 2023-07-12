package es.dmr.uimp.realtime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.HashMap
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.udf

import java.text.SimpleDateFormat

object InvoicePipeline {

  case class Purchase(invoiceNo: String, quantity: Int, invoiceDate: String,
                      unitPrice: Double, customerID: String, country: String)

  case class Invoice(invoiceNo: String, avgUnitPrice: Double,
                     minUnitPrice: Double, maxUnitPrice: Double, time: Double,
                     numberItems: Double, lastUpdated: Long, lines: Int, customerId: String, state: Int)

  // State to indicate estate of invoice
  object InvoiceStatus {
    val NonEmited = 0
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

    val bcKmeansModel = sc.broadcast(kmeansModel)
    val bcBisectModel = sc.broadcast(bisectModel)
    val bcKmeansThreshold = sc.broadcast(kmeansThreshold)
    val bcBisectThreshold = sc.broadcast(bisectThreshold)

    // Broadcast the Kafka brokers to make them available in all nodes
    val broadcastBrokers = ssc.sparkContext.broadcast(brokers)

    // TODO: Build pipeline

    // Connect to Kafka and parse input data
    val purchasesFeed = connectToPurchases(ssc, zkQuorum, group, topics, numThreads)

    // Parse the input data and create the purchases DStream
    val purchasesDStream = parsePurchases(purchasesFeed)
    purchasesDStream.print(5)

    // Create a DStream of invoices and update the state
    val invoicesDStream = purchasesDStream.updateStateByKey(updateInvoice)
    invoicesDStream.print(5)

    // TODO: rest of pipeline

    val invalidDStream = invalidPipeline(invoicesDStream)
    invalidDStream.foreachRDD(rdd => publishToKafka("invalid_invoices")(broadcastBrokers)(rdd))

    val WINDOW_LENGTH = 1
    val SLIDE_INTERVAL = 20
    val cancelDStream = cancelationPipeline(invoicesDStream, WINDOW_LENGTH, SLIDE_INTERVAL)
    cancelDStream.foreachRDD(rdd => publishToKafka("cancelations_ma")(broadcastBrokers)(rdd))

    // TODO: Find anomalies using KMeans
    // anomaliesKmeans.foreachRDD(rdd => publishToKafka("anomalies_kmeans")(broadcastBrokers)(rdd))
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

  // ------------------- PIPELINES FUNCTIONS -------------------

  /**
   * Processes a stream of invoices and generates a stream of canceled invoice counts within a specified window.
   *
   * @param invoices       The input stream of invoices as (key, value) pairs, where the key is a string identifier
   *                       and the value is an Invoice object.
   * @param window_length  The length of the sliding window in minutes for counting canceled invoices. Default is 8 minutes.
   * @param slide_interval The slide interval in seconds for the sliding window. Default is 20 seconds.
   * @return The stream of canceled invoice counts within the specified window, transformed for publishing in Kafka.
   */
  def cancelationPipeline(invoices: DStream[(String, Invoice)], window_length: Int = 8, slide_interval: Int = 20): DStream[(String, String)] = {
    // Filter canceled and nonEmitted invoices
    val filteredInvoices = invoices.filter(inv => {
      inv._1.startsWith("C") && inv._2.state != InvoiceStatus.NonEmited
    })
    // Count by window
    val countCanceledDStream = filteredInvoices.countByWindow(Minutes(window_length), Seconds(slide_interval))

    // Transform to publish in kafka
    countCanceledDStream.transform { rdd =>
      rdd.map(count =>
        (count.toString, "Facturas canceladas en " + window_length.toString + " minutos: " + count.toString)
      )
    }
  }

  /**
   * Processes a stream of invoices and filters out invalid invoices that are emitting but have invalid data.
   *
   * @param invoices The input stream of invoices as (key, value) pairs, where the key is a string identifier
   *                 and the value is an Invoice object.
   */
  def invalidPipeline(invoices: DStream[(String, Invoice)]) = {
    // Filter invoices which are emitting and are invalid
    val invalidInvoices = invoices.filter(inv => inv._2.state == InvoiceStatus.Emitting && isInvalid(inv._2))
    invalidInvoices.transform(rdd => rdd.map(inv => (inv._1, inv._2.toString)))
    // .foreachRDD(rdd => publishToKafka("facturas_erroneas")(kafkaBrokers)(rdd))

  }

  // Function to identify erroneous invoices
  private def isInvalid(invoice: Invoice): Boolean = {
    // Check for null values
    for (field <- invoice.productIterator) {
      if (field == null) return true
    }
    // Check for negative values
    if (invoice.avgUnitPrice <= 0 || invoice.time < 0 || invoice.numberItems <= 0 || invoice.lines <= 0) return true

    false
  }

  // ------------------- STATE METHODS -------------------

  /**
   * Updates the running invoice with new purchases and returns the updated invoice.
   *
   * @param newPurchases   A sequence of new purchases to be added to the invoice.
   * @param runningInvoice An optional running invoice that needs to be updated.
   * @return An optional updated invoice, or None if the invoice is to be removed from the state.
   */
  private def updateInvoice(newPurchases: Seq[Purchase], runningInvoice: Option[Invoice]): Option[Invoice] = {
    // If there is no running invoice, create a new one
    if(runningInvoice.isEmpty) {
      return Some(newInvoice(newPurchases))
    }

    // Compute time values
    val currentTime: Long = System.currentTimeMillis()
    val maxTimeThreshold: Long = 8 * 60 * 1000 // 8 minutes in milliseconds
    val minTimeThreshold: Long = 40 * 1000 // 40 seconds in milliseconds
    val timeDiff: Long = currentTime - runningInvoice.get.lastUpdated

    // If the running invoice is emitted and the time threshold has passed, remove it from the state
    if(runningInvoice.get.state == InvoiceStatus.Emitted && timeDiff > maxTimeThreshold) {
      // Remove the invoice from the state
      return None
    }

    // If the running invoice is emitting set it to emitted
    if(runningInvoice.get.state == InvoiceStatus.Emitting) {
      val emittedInvoice = runningInvoice.get.copy(state = InvoiceStatus.Emitted)
      return Some(emittedInvoice)
    }

    // If the running invoice is not emitted and the time threshold has passed, emit it
    if(runningInvoice.get.state == InvoiceStatus.NonEmited && timeDiff > minTimeThreshold) {
      val emittedInvoice = runningInvoice.get.copy(state = InvoiceStatus.Emitting)
      return Some(emittedInvoice)
    }

    // If there is new purchases, update the invoice
    if(!newPurchases.isEmpty) {
      // Update the invoice with the new values
      val updatedInvoice = updateValuesInvoice(newPurchases, runningInvoice.get)
      return Some(updatedInvoice)
    }

    runningInvoice
  }

  private def newInvoice(purchases: Seq[Purchase]): Invoice = {
    // Compute new values for the invoice based on the new purchases and the current state
    val lines = purchases.size
    val numberItems = purchases.map(_.quantity).sum
    val avgUnitPrice = purchases.map(_.unitPrice).sum / lines
    val minUnitPrice = purchases.map(_.unitPrice).min
    val maxUnitPrice = purchases.map(_.unitPrice).max
    val customerID = purchases.head.customerID
    val time = getHour(purchases.head.invoiceDate)
    val lastUpdated = System.currentTimeMillis()
    // Create new invoice
    Invoice(purchases.head.invoiceNo, avgUnitPrice, minUnitPrice, maxUnitPrice, time, numberItems, lastUpdated, lines, customerID, InvoiceStatus.NonEmited)
  }

  private def updateValuesInvoice(newPurchases: Seq[Purchase], runningInvoice: Invoice): Invoice = {
    // Compute new values for the invoice based on the new purchases and the current state
    val lines = newPurchases.size + runningInvoice.lines // Number of purchases in the invoice
    val numberItems = newPurchases.map(_.quantity).sum + runningInvoice.numberItems // Number of items in the invoice

    // If unitprice is null in purchases, it will be null in invoice
    var avgUnitPrice, minUnitPrice, maxUnitPrice = -1.0

    // If unitprice is greater than 0, compute new values
    if(runningInvoice.avgUnitPrice > 0 ) {
      avgUnitPrice = (newPurchases.map(_.quantity).sum + runningInvoice.avgUnitPrice) / lines
      minUnitPrice = Math.min(newPurchases.map(_.unitPrice).min, runningInvoice.minUnitPrice)
      maxUnitPrice = Math.max(newPurchases.map(_.unitPrice).max, runningInvoice.maxUnitPrice)
    }

    val lastUpdated = System.currentTimeMillis()
    val customerId = newPurchases.head.customerID
    // Convert to date and extract hour
    val time = getHour(newPurchases.head.invoiceDate)

    Invoice(newPurchases.head.invoiceNo, avgUnitPrice, minUnitPrice, maxUnitPrice, time, numberItems, lastUpdated, lines, customerId, runningInvoice.state)
  }

  private def getHour(date: String): Double = {
    if (date != null && date.nonEmpty) {
      val hour = date.substring(10).split(":")(0)
      if (hour != null && hour.nonEmpty) {
        return hour.trim.toDouble
      }
    }
    -1.0
  }
}
