package es.dmr.uimp.realtime

import es.dmr.uimp.realtime.InvoicePipeline.{Invoice, InvoiceStatus, Purchase}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.{BisectingKMeansModel, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream

object PipelineFunctions {
  // --------------------------------------- CANCELLATION PIPELINE FUNCTIONS ---------------------------------------

  /**
   * Processes a stream of invoices and generates a stream of canceled invoice counts within a specified window.
   *
   * @param invoices       The input stream of invoices as (key, value) pairs, where the key is a string identifier
   *                       and the value is an Invoice object.
   * @param window_length  The length of the sliding window in minutes for counting canceled invoices. Default is 8 minutes.
   * @param slide_interval The slide interval in seconds for the sliding window. Default is 20 seconds.
   * @return The stream of canceled invoice counts within the specified window, transformed for publishing in Kafka.
   */
  def cancellationPipeline(invoices: DStream[(String, Invoice)], window_length: Int = 8, slide_interval: Int = 60): DStream[(String, String)] = {
    // Filter canceled and nonEmitted invoices
    val filteredInvoices = invoices.filter(inv => {
      inv._1.startsWith("C") && inv._2.state != InvoiceStatus.NonEmitted
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

  // --------------------------------------- INVALID PIPELINE FUNCTIONS ---------------------------------------

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

  // --------------------------------------- CLUSTERING PIPELINE FUNCTIONS ---------------------------------------

  /**
   * Processes a stream of invoices and filters out invoices that are emitting and are anomalous.
   * An invoice is considered anomalous if it is emitting and its features are outside the threshold.
   *
   * @param invoices  The input stream of invoices as (key, value) pairs, where the key is a string identifier
   *                  and the value is an Invoice object.
   * @param model     The clustering model to use for anomaly detection.
   * @param threshold The threshold to use for anomaly detection.
   * @return The stream of anomalous invoices, transformed for publishing in Kafka.
   */
  def clusteringPipeline(invoices: DStream[(String, Invoice)], model: Either[KMeansModel, BisectingKMeansModel], threshold: Broadcast[Double]): DStream[(String, String)] = {
    // Filter invoices which are emitting
    val emittingInvoices = invoices.filter(inv => inv._2.state == InvoiceStatus.Emitting)
    // Run KMeans on the invoices to get the distance to the centroid
    val invoicesWithDistance = emittingInvoices.map(inv => (inv._1, inv._2, model match {
      case Left(kmeansModel) => kmeansModel.predict(Vectors.dense(inv._2.avgUnitPrice, inv._2.minUnitPrice, inv._2.maxUnitPrice, inv._2.time, inv._2.numberItems))
      case Right(bisectingKMeansModel) => bisectingKMeansModel.predict(Vectors.dense(inv._2.avgUnitPrice, inv._2.minUnitPrice, inv._2.maxUnitPrice, inv._2.time, inv._2.numberItems))
    }))
    invoicesWithDistance.print(10)
    // Filter invoices which are emitting and are anomalous
    val anomalousInvoices = invoicesWithDistance.filter(inv => inv._3 > threshold.value)
    // Transform to publish in kafka
    anomalousInvoices.transform { rdd =>
      rdd.map(inv =>
        (inv._1, "Factura " + inv._2.invoiceNo + " con distancia " + inv._3.toString)
      )
    }
  }
  // --------------------------------------- STATE MANAGEMENT METHODS ---------------------------------------

  /**
   * Updates the running invoice with new purchases and returns the updated invoice.
   *
   * @param newPurchases   A sequence of new purchases to be added to the invoice.
   * @param runningInvoice An optional running invoice that needs to be updated.
   * @return An optional updated invoice, or None if the invoice is to be removed from the state.
   */
  def updateInvoice(newPurchases: Seq[Purchase], runningInvoice: Option[Invoice]): Option[Invoice] = {
    // If there is no running invoice, create a new one
    if (runningInvoice.isEmpty) {
      return Some(newInvoice(newPurchases))
    }

    // Compute time values
    val currentTime: Long = System.currentTimeMillis()
    val maxTimeThreshold: Long = 8 * 60 * 1000 // 8 minutes in milliseconds
    val minTimeThreshold: Long = 40 * 1000 // 40 seconds in milliseconds
    val timeDiff: Long = currentTime - runningInvoice.get.lastUpdated

    // If the running invoice is emitted and the time threshold has passed, remove it from the state
    if (runningInvoice.get.state == InvoiceStatus.Emitted && timeDiff > maxTimeThreshold) {
      // Remove the invoice from the state
      return None
    }

    // If the running invoice is emitting set it to emitted
    if (runningInvoice.get.state == InvoiceStatus.Emitting) {
      val emittedInvoice = runningInvoice.get.copy(state = InvoiceStatus.Emitted)
      return Some(emittedInvoice)
    }

    // If the running invoice is not emitted and the time threshold has passed, emit it
    if (runningInvoice.get.state == InvoiceStatus.NonEmitted && timeDiff > minTimeThreshold) {
      val emittedInvoice = runningInvoice.get.copy(state = InvoiceStatus.Emitting)
      return Some(emittedInvoice)
    }

    // If there is new purchases, update the invoice
    if (!newPurchases.isEmpty) {
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
    Invoice(purchases.head.invoiceNo, avgUnitPrice, minUnitPrice, maxUnitPrice, time, numberItems, lastUpdated, lines, customerID, InvoiceStatus.NonEmitted)
  }

  private def updateValuesInvoice(newPurchases: Seq[Purchase], runningInvoice: Invoice): Invoice = {
    // Compute new values for the invoice based on the new purchases and the current state
    val lines = newPurchases.size + runningInvoice.lines // Number of purchases in the invoice
    val numberItems = newPurchases.map(_.quantity).sum + runningInvoice.numberItems // Number of items in the invoice

    // If unitprice is null in purchases, it will be null in invoice
    var avgUnitPrice, minUnitPrice, maxUnitPrice = -1.0

    // If unitprice is greater than 0, compute new values
    if (runningInvoice.avgUnitPrice > 0) {
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
