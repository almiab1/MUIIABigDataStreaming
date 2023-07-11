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

import java.text.SimpleDateFormat

object InvoicePipeline {

  case class Purchase(invoiceNo: String, quantity: Int, invoiceDate: String,
                      unitPrice: Double, customerID: String, country: String)

  case class Invoice(invoiceNo: String, avgUnitPrice: Double,
                     minUnitPrice: Double, maxUnitPrice: Double, time: Double,
                     numberItems: Double, lastUpdated: Long, lines: Int, customerId: String)

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
    val invoicesDStream = purchasesDStream.updateStateByKey(updateInvoice)
    invoicesDStream.print(5)

    // Implement updateStateByKey to keep track invoices


    // TODO: rest of pipeline

    // TODO: Filter invalid invoices
    // val invalidDStream = invalidInvoicePipeline(purchasesDStream)
    // anomaliesBisection.foreachRDD(rdd => publishToKafka("invalid_invoices")(broadcastBrokers)(rdd))

    // TODO: Filter canceled invoices
    // val WINDOW_LENGTH = 8
    // val SLIDE_INTERVAL = 20
    // val cancelDStream = cancelationPipeline(purchasesDStream, WINDOW_LENGTH, SLIDE_INTERVAL)
    // cancelDStream.foreachRDD(rdd => publishToKafka("cancelations_ma")(broadcastBrokers)(rdd))

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

  // Parse the input data and create the purchases DStream
  def parsePurchases(purchasesFeed: DStream[(String, String)]): DStream[(String, Purchase)] = {
    val purchases = purchasesFeed.map { case (_, line) =>
      val Array(invoiceNo, _, _, quantity, invoiceDate, unitPrice, customerID, country) = line.split(",")
      val purchase = Purchase(invoiceNo, quantity.toInt, invoiceDate, unitPrice.toDouble, customerID, country)
      (invoiceNo, purchase)
    }

    purchases
  }

  // ------------------- PIPELINES METHODS -------------------

  // Cancelation Pipeline
  // 1 - Receive a DStream[(String, Invoice)].
  // 2 - Filter this DStream to keep only the cancelled ones (the ones that start with C in their InvoiceNo)
  // 3 - I apply a countByWindow that should give me a DStream with RDD that include the amount of the same ones.
  // 4 - I make a foreach to publish them in kafka with the function publish to kafka
  def cancelationPipeline(invoices: DStream[(String, Invoice)],
                          window_length: Int = 8,
                          slide_interval: Int = 20): DStream[(String, String)] = {
    // Filter canceled invoices
    val filteredInvoices = invoices.filter(p => p._2.invoiceNo.startsWith("C"))
    // Count by window
    val countCanceledDStream = filteredInvoices.countByWindow(Minutes(window_length), Seconds(slide_interval))

    // Transform to publish in kafka
    countCanceledDStream.transform { rdd =>
      rdd.map(count =>
        (count.toString, "Facturas canceladas en " + window_length.toString + " minutos: " + count.toString)
      )
    }
  }

  // Filter invalid invoices
  def invalidPipeline(invoices: DStream[(String, Invoice)]) = {
    // Filter canceled invoices
    invoices.filter(inv => isErroneous(inv._2))
    // .foreachRDD(rdd => publishToKafka("facturas_erroneas")(kafkaBrokers)(rdd))

  }

  // Define a function to identify erroneous invoices
  private def isErroneous(invoice: Invoice): Boolean = {
    // Check for null values
    for (field <- invoice.productIterator) {
      if (field == null) return true
    }

    false
  }

  // Función de actualización
  def updateInvoice(newPurchases: Seq[Purchase], runningInvoice: Option[Invoice]): Option[Invoice] = {

    if (newPurchases.isEmpty) {
      // If there are no new purchases for this invoice, keep the current state
      runningInvoice
    } else {
      // Compute new values for the invoice based on the new purchases and the current state
      val invoice = runningInvoice.getOrElse(Invoice(newPurchases.head.invoiceNo, 0, 0, 0, 0, 0, 0, 0, newPurchases.head.customerID))

      // Update the invoice with the new values
      val lines = invoice.lines + newPurchases.size
      val numberItems = newPurchases.map(_.quantity).sum + invoice.numberItems
      val avgUnitPrice = (newPurchases.map(_.quantity).sum + invoice.avgUnitPrice) / invoice.lines
      val minUnitPrice = Math.min(newPurchases.map(_.unitPrice).min, invoice.minUnitPrice)
      val maxUnitPrice = Math.max(newPurchases.map(_.unitPrice).max, invoice.maxUnitPrice)
      val lastUpdated = System.currentTimeMillis()
      val customerId = newPurchases.head.customerID

      val updatedInvoice = Invoice(invoice.invoiceNo, avgUnitPrice, minUnitPrice, maxUnitPrice, 0, numberItems, lastUpdated, lines, customerId)

      Some(updatedInvoice)
    }
  }

  // Invoice
  //  invoiceNo: String,
  //  avgUnitPrice: Double,
  //  minUnitPrice: Double,
  //  maxUnitPrice: Double,
  //  time: Double,
  //  numberItems: Double,
  //  lastUpdated: Long,
  //  lines: Int,
  //  customerId: String)

  // Purchase
  //  invoiceNo: String,
  //  quantity: Int,
  //  invoiceDate: String,
  //  unitPrice: Double,
  //  customerID: String,
  //  country: String)
}
