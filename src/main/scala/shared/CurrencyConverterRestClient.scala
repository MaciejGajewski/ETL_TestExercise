package shared

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

import scala.util.parsing.json._

object CurrencyConverterRestClient {

  def get(args: Array[String]) {

    // (1) get the content from the yahoo weather api url
    val content = getRestContent("http://localhost:8080/currency?price=123.45")

    val parsed = JSON.parseFull(content)
    val price = parsed match {
      case Some(m: Map[String, Double]) => m.get("price").get
      case _ => None
    }

    println(parsed)
    println(price)

  }

  /**
    * Returns the text content from a REST URL. Returns a blank String if there
    * is a problem.
    */
  def getRestContent(url:String): String = {
    val httpClient = new DefaultHttpClient()
    val httpResponse = httpClient.execute(new HttpGet(url))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    httpClient.getConnectionManager().shutdown()
    return content
  }

}
