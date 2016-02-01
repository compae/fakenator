package com.stratio.runners

import java.io.File
import java.lang.Double
import java.util.UUID

import com.stratio.decision.api.StratioStreamingAPIFactory
import com.stratio.decision.api.messaging.{ColumnNameValue, ColumnNameType}
import com.stratio.decision.commons.constants.ColumnType
import com.stratio.decision.commons.exceptions.StratioStreamingException
import scala.collection.JavaConversions._
import scala.io.Source

object FakenatorRunner {

  val stratioStreamingAPI = StratioStreamingAPIFactory
    .create
    .withServerConfig("localhost",9092, "localhost", 2181)
    .init

  val streamName = "c_orders"

  println(streamName)

  def main(args: Array[String]): Unit = {


    insertOrderData

  }

  // shell stream creation
  // create --stream c_orders --definition "order_id.string, day_time_zone.string, client_id.integer, latitude
  // .double, longitude.double, payment_method.string, credit_card.long, shopping_center.string, employee
  // .integer, total_amount.double, total_products.integer"

    def insertOrderData(): Unit = {


    val f = new File(getClass.getClassLoader.getResource("order_data.csv").getPath)
    var count = 0

    Source
      .fromFile(f)
      .getLines
      .drop(1)
      .foreach { line =>

        val values = line.split(",")

//    order_id,timestamp,client_id,latitude,longitude,payment_method,credit_card,shopping_center,employee,total_amount,total_products

       val orderIdColumnValue = new ColumnNameValue("order_id",  values(0))
       val timestampColumnValue = new ColumnNameValue("day_time_zone",  values(1))
       val clientIdColumnValue = new ColumnNameValue("client_id",  new Integer(values(2)))
       val latitudeColumnValue = new ColumnNameValue("latitude",  new Double(values(3)))
       val longitudeColumnValue = new ColumnNameValue("longitude",  new Double(values(4)))
       val paymentMethodColumnValue = new ColumnNameValue("payment_method",  values(5))
       val creditCardColumnValue = new ColumnNameValue("credit_card",  new java.lang.Long(values(6)))
       val shoppingCenterColumnValue = new ColumnNameValue("shopping_center",  values(7))
       val employeeColumnValue = new ColumnNameValue("employee",  new Integer(values(8)))
       val totalAmountColumnValue = new ColumnNameValue("total_amount",  new Double(values(9)))
       val totalProductsColumnValue = new ColumnNameValue("total_products",  new Integer(values(10)))


       var streamData = Seq(orderIdColumnValue, timestampColumnValue, clientIdColumnValue, latitudeColumnValue,
         longitudeColumnValue, paymentMethodColumnValue, creditCardColumnValue, shoppingCenterColumnValue,
         employeeColumnValue, totalAmountColumnValue, totalProductsColumnValue)

       stratioStreamingAPI.insertData(streamName, streamData)

        // Missing stream fields in csv file

        //        "type": "string",
        //        "name": "city"

        //        "type": "string",
        //        "name": "country"

        //        "type": "string",
        //        "name": "channel"

        //        "type": "string",
        //        "name": "order_size"

      }

  }
}
