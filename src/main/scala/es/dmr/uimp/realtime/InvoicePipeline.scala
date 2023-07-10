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
import es.dmr.uimp.clustering.KMeansClusterInvoices
import es.dmr.uimp.clustering.BisectingKMeansClusterInvoices
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object InvoicePipeline {

  case class Purchase(invoiceNo : String, quantity : Int, invoiceDate : String,
                      unitPrice : Double, customerID : String, country : String)

  case class Invoice(invoiceNo : String, avgUnitPrice : Double,
                     minUnitPrice : Double, maxUnitPrice : Double, time : Double,
                     numberItems : Double, lastUpdated : Long, lines : Int, customerId : String)

  def main(args: Array[String]) {

    val Array(modelFile, thresholdFile, modelFileBisect, thresholdFileBisect, zkQuorum, group, topics, numThreads, brokers) = args
    val sparkConf = new SparkConf().setAppName("InvoicePipeline")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(20))
    ssc.sparkContext.setLogLevel("WARN")

    // Checkpointing
    ssc.checkpoint("./checkpoint")

    // TODO: Load model and broadcast

    val modelKmeans = loadKMeansAndThreshold(sc,modelFile,thresholdFile)
    val kmeans_centroid = ssc.sparkContext.broadcast(modelKmeans._1)
    val kmeans_threshold = ssc.sparkContext.broadcast(modelKmeans._2)

    val modelBisect = loadBisectKMeansAndThreshold(sc,modelFileBisect,thresholdFileBisect)
    val bisect_centroid = ssc.sparkContext.broadcast(modelBisect._1)
    val bisect_threshold = ssc.sparkContext.broadcast(modelBisect._2)
    
    // Broadcast the Kafka brokers to make them available in all nodes
    val broadcastBrokers = ssc.sparkContext.broadcast(brokers)
    println("")
    println("######################################################################################")
    println("Brokers")
    println(brokers)
    println("######################################################################################")
    println("")

    // TODO: Build pipeline


    // connect to kafka
    val purchasesFeed = connectToPurchases(ssc, zkQuorum, group, topics, numThreads)
    // Print the raw purchases
    purchasesFeed.print()

    purchasesFeed.foreachRDD { rdd =>
      val invoices = rdd.map(_._2) // Consideramos sólo el cuerpo del mensaje (factura)
      // TODO: Lógica para identificar facturas problemáticas, canceladas, y aplicar clustering
      invoices.collect().foreach(println)
    }

    // Parse the raw purchases into Purchase objects
    // val purchases = purchasesFeed.map(record => {
    //     val fields = record._2.split(",")
    //     val invoiceNo = fields(0)
    //     (invoiceNo, List(fields.slice(1, fields.length).mkString(",")))
    // })

    // // Group the purchases by invoiceNo
    // val updateFunc = (newValues: Seq[List[String]], runningValue: Option[List[String]]) => {
    //   val current = runningValue.getOrElse(List())
    //   val updated = current ++ newValues.flatten
    //   Some(updated)
    // }
    // // Update the state of the grouped invoices
    // val invoiceState = purchases.updateStateByKey[List[String]](updateFunc)

    // // Print the grouped invoices
    // invoiceState.print()

    // Filter for invalid invoices
    // val invalidInvoices = invoiceState.filter(record => {
    //     val fields = record._2.head.split(",")
    //     val invoiceDate = fields(3)  // assuming 4th field is InvoiceDate
    //     val customerID = fields(5)  // assuming 6th field is CustomerID
    //     invoiceDate.isEmpty || customerID.isEmpty
    // }).map(invoice => (invoice._1, invoice._2.mkString(",")))  // Formatting the invoice data for Kafka☺

    // invalidInvoices.print()

    // // Publish invalid invoices to Kafka
    // invalidInvoices.foreachRDD(rdd => {
    //   publishToKafka("invalid_invoices")(broadcastBrokers)(rdd)
    // })

    // TODO: Find anomalies using KMeans
    // TODO: Find anomalies using Bisecting KMeans
    // TODO: Filter canceled invoices

    // TODO: rest of pipeline


    // TOPICS -------------------
    // anomalies_kmeans
    // anomalies_kmeans_bisect
    // cancelations_ma - “cancelaciones” - Recuerde, en el topic de cancelaciones se deben publicar el número de cancelaciones en los últimos 8 minutos, no mostrarlas todas.
    // invalid_invoices - facturas erroneas



    ssc.start() // Start the computation
    ssc.awaitTermination()
  }

  def publishToKafka(topic : String)(kafkaBrokers : Broadcast[String])(rdd : RDD[(String, String)]) = {
    rdd.foreachPartition( partition => {
      val producer = new KafkaProducer[String, String](kafkaConf(kafkaBrokers.value))
      partition.foreach( record => {
        producer.send(new ProducerRecord[String, String](topic, record._1,  record._2.toString))
      })
      producer.close()
    })
  }

  def kafkaConf(brokers : String) = {
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props
  }

  /**
    * Load the model information: centroid and threshold
    */
  def loadKMeansAndThreshold(sc: SparkContext, modelFile : String, thresholdFile : String) : Tuple2[KMeansModel,Double] = {
    val kmeans = KMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map{line => line.toDouble}.first()

    (kmeans, threshold)
  }

  def loadBisectKMeansAndThreshold(sc: SparkContext, modelFile : String, thresholdFile : String) : Tuple2[BisectingKMeansModel,Double] = {
    val bisectModel = BisectingKMeansModel.load(sc, modelFile)
    // parse threshold file
    val rawData = sc.textFile(thresholdFile, 20)
    val threshold = rawData.map{line => line.toDouble}.first()

    (bisectModel, threshold)
  }


  def connectToPurchases(ssc: StreamingContext, zkQuorum : String, group : String,
                         topics : String, numThreads : String): DStream[(String, String)] ={

    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
  }

}
