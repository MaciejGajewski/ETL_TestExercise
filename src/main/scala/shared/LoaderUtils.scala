package shared

import java.io.{IOException, InterruptedIOException}
import java.net.UnknownHostException

import javax.net.ssl.SSLException
import org.apache.http.HttpEntityEnclosingRequest
import org.apache.http.client.HttpRequestRetryHandler
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.conn.ConnectTimeoutException
import org.apache.http.impl.client.HttpClients
import org.apache.http.protocol.HttpContext
import org.apache.log4j.{LogManager, Logger, PropertyConfigurator}

import scala.util.parsing.json.JSON

object LoaderUtils {

  PropertyConfigurator.configure("src/main/resources/log4j.properties")
  val log: Logger = LogManager.getRootLogger

  def getPriceFromJson(jsonString: String): Double = {

    val parsed = JSON.parseFull(jsonString)
    val price = parsed match {
      case Some(m: Map[String, Double]) => m("price")
      case _ => None
    }

    price.toString.toDouble
  }

  def getConvertedCurrency(price: Double): Double = {
    val currencyConvServiceUri = "http://localhost:8080/currency?price=" + price.toString

    val myRetryHandler = new HttpRequestRetryHandler() {
      def retryRequest(exception: IOException, executionCount: Int, context: HttpContext): Boolean = {
        if (executionCount >= 0) { // Do not retry if over max retry count
          return false
        }
        if (exception.isInstanceOf[InterruptedIOException]) { // Timeout
          return false
        }
        if (exception.isInstanceOf[UnknownHostException]) { // Unknown host
          return false
        }
        if (exception.isInstanceOf[ConnectTimeoutException]) { // Connection refused
          return false
        }
        if (exception.isInstanceOf[SSLException]) { // SSL handshake exception
          return false
        }
        val clientContext = HttpClientContext.adapt(context)
        val request = clientContext.getRequest
        val idempotent = !request.isInstanceOf[HttpEntityEnclosingRequest]
        if (idempotent) { // Retry if the request is considered idempotent
          return true
        }
        false
      }
    }

    val httpClient = HttpClients.custom().setRetryHandler(myRetryHandler).build
    val httpGet = new HttpGet(currencyConvServiceUri)

    var content = ""
    val httpResponse = httpClient.execute(httpGet)
    try {
      val entity = httpResponse.getEntity
      if (entity != null) {
        val inputStream = entity.getContent
        try
          content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        finally
          inputStream.close()
      }
    }
    finally {
      httpResponse.close()
    }

    getPriceFromJson(content)
  }


  def time[R](block: => R): (Double, R) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val timediff = (t1 - t0) / 1e9
    log.info("Elapsed time: " + timediff + " seconds")
    (timediff, result)
  }

  def main(args: Array[String]): Unit = {
    val testPrice = getConvertedCurrency(213.0)
    log.info("Test Price is " + testPrice)
  }
}
