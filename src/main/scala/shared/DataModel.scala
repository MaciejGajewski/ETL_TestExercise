package shared

@SerialVersionUID(100L)
case class DataModel (
                      var observed_date_min_as_infaredate: Int,
                      var observed_date_max_as_infaredate: Int,
                      var full_weeks_before_departure: Int,
                      var carrier_id: Int,
                      var searched_cabin_class: String,
                      var booking_site_id: Int,
                      var booking_site_type_id: Int,
                      var is_trip_one_way: Int,
                      var trip_origin_airport_id: Int,
                      var trip_destination_airport_id: Int,
                      var trip_min_stay: Option[Int],
                      var trip_price_min: Double,
                      var trip_price_max: Double,
                      var trip_price_avg: Double,
                      var aggregation_count: Int,
                      var out_flight_departure_date_as_infaredate: Int,
                      var out_flight_departure_time_as_infaretime: Int,
                      var out_flight_time_in_minutes: Option[Int],
                      var out_sector_count: Int,
                      var out_flight_sector_1_flight_code_id: Int,
                      var out_flight_sector_2_flight_code_id: Option[Int],
                      var out_flight_sector_3_flight_code_id: Option[Int],
                      var home_flight_departure_date_as_infaredate: Option[Int],
                      var home_flight_departure_time_as_infaretime: Option[Int],
                      var home_flight_time_in_minutes: Option[Int],
                      var home_sector_count: Int,
                      var home_flight_sector_1_flight_code_id: Option[Int],
                      var home_flight_sector_2_flight_code_id: Option[Int],
                      var home_flight_sector_3_flight_code_id: Option[Int],
                      var observation_week: Int,
                      var trip_price_avg_2: Double
                    ) extends Serializable with Product {

  def this(o: Array[String]) = this(
    o(0).toInt,
    o(1).toInt,
    o(2).toInt,
    o(3).toInt,
    o(4),
    o(5).toInt,
    o(6).toInt,
    o(7).toInt,
    o(8).toInt,
    o(9).toInt,
    if (o(10).length > 0) Some(o(10).toInt) else None,
    o(11).toDouble,
    o(12).toDouble,
    o(13).toDouble,
    o(14).toInt,
    o(15).toInt,
    o(16).toInt,
    if (o(17).length > 0) Some(o(17).toInt) else None,
    o(18).toInt,
    o(19).toInt,
    if (o(20).length > 0) Some(o(20).toInt) else None,
    if (o(21).length > 0) Some(o(21).toInt) else None,
    if (o(22).length > 0) Some(o(22).toInt) else None,
    if (o(23).length > 0) Some(o(23).toInt) else None,
    if (o(24).length > 0) Some(o(24).toInt) else None,
    o(25).toInt,
    if (o(26).length > 0) Some(o(26).toInt) else None,
    if (o(27).length > 0) Some(o(27).toInt) else None,
    if (o(28).length > 0) Some(o(28).toInt) else None,
    Math.round( ( (o(0).toInt - 3)/7 ).floatValue() ) + 1,   // 01.01.2000 was Sat
    0.0
  )

  def canEqual(that: Any) = that.isInstanceOf[DataModel]

  def productArity = 31

  def productElement(idx: Int) = idx match {
    case 0 => observed_date_min_as_infaredate
    case 1 => observed_date_max_as_infaredate
    case 2 => full_weeks_before_departure
    case 3 => carrier_id
    case 4 => searched_cabin_class
    case 5 => booking_site_id
    case 6 => booking_site_type_id
    case 7 => is_trip_one_way
    case 8 => trip_origin_airport_id
    case 9 => trip_destination_airport_id
    case 10 => trip_min_stay
    case 11 => trip_price_min
    case 12 => trip_price_max
    case 13 => trip_price_avg
    case 14 => aggregation_count
    case 15 => out_flight_departure_date_as_infaredate
    case 16 => out_flight_departure_time_as_infaretime
    case 17 => out_flight_time_in_minutes
    case 18 => out_sector_count
    case 19 => out_flight_sector_1_flight_code_id
    case 20 => out_flight_sector_2_flight_code_id
    case 21 => out_flight_sector_3_flight_code_id
    case 22 => home_flight_departure_date_as_infaredate
    case 23 => home_flight_departure_time_as_infaretime
    case 24 => home_flight_time_in_minutes
    case 25 => home_sector_count
    case 26 => home_flight_sector_1_flight_code_id
    case 27 => home_flight_sector_2_flight_code_id
    case 28 => home_flight_sector_3_flight_code_id
    case 29 => observation_week
    case 30 => trip_price_avg_2
  }
}

object BaseEncoder {
  implicit def baseEncoder: org.apache.spark.sql.Encoder[DataModel] = org.apache.spark.sql.Encoders.kryo[DataModel]
}

