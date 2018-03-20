package shared

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.log4j.{LogManager, PropertyConfigurator}

import scala.util.parsing.json.JSON

object LoaderUtils {

  PropertyConfigurator.configure("src/main/resources/log4j.properties")
  val log = LogManager.getRootLogger()

  def getPriceFromJson(jsonString: String): Double = {

    val parsed = JSON.parseFull(jsonString)
    val price = parsed match {
      case Some(m: Map[String, Double]) => m.get("price").get
      case _ => None
    }

    return price.toString.toDouble
  }

  def getConvertedCurrency(price: Double): Double = {
    val httpClient = new DefaultHttpClient()
    val currencyConvServiceUri = "http://localhost:8080/currency?price=" + price.toString
    val httpResponse = httpClient.execute(new HttpGet(currencyConvServiceUri))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    httpClient.getConnectionManager().shutdown()

    return getPriceFromJson(content)
  }


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val timediff = t1 - t0
    log.info("Elapsed time: " + (timediff) / 1e9 + " seconds")
    result
  }

}
