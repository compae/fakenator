package com.stratio.runners

import java.lang.Double
import java.util.UUID

import com.stratio.decision.api.StratioStreamingAPIFactory
import com.stratio.decision.api.messaging.{ColumnNameValue, ColumnNameType}
import com.stratio.decision.commons.constants.ColumnType
import com.stratio.decision.commons.exceptions.StratioStreamingException
import scala.collection.JavaConversions._

object FakenatorRunner {

  val stratioStreamingAPI = StratioStreamingAPIFactory
    .create
    .withServerConfig("localhost",9092, "localhost", 2181)
    .init

  val streamName = "drools_poc_stream"

  println(streamName)

  def main(args: Array[String]): Unit = {
    /*
    if(args.size == 0) {
      println(s">> Creating stream: $streamName")
      createStream
    }
*/

     testCreateStream


    //insertData
    //insertDataTopic("topic_A")
  }

  def createStream(): Unit = {
    val name = new ColumnNameType("name", ColumnType.STRING)
    val data1 = new ColumnNameType("data1", ColumnType.INTEGER)
    val data2 = new ColumnNameType("data2", ColumnType.FLOAT)

    val columnList = Seq(name, data1, data2)

    try {
      stratioStreamingAPI.createStream(streamName, columnList)
      stratioStreamingAPI.saveToMongo(streamName)
      stratioStreamingAPI.listenStream(streamName)

      UUID.randomUUID()
    } catch {
      case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }
  }



  def testCreateStream():Unit = {

    val data1 = new ColumnNameType("col1", ColumnType.INTEGER)
    val data2 = new ColumnNameType("col2", ColumnType.STRING)

    val columnList = Seq( data1, data2)

    try {
      stratioStreamingAPI.createStream(streamName, columnList)

      UUID.randomUUID()
    } catch {
      case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }


  }


  def insertDataTopic(topicName:String): Unit = {

    //var firstColumnValue = new ColumnNameValue("name", "test")
    //var secondColumnValue = new ColumnNameValue("data1", new Integer(1))
    //var thirdColumnValue = new ColumnNameValue("data2", new Double(2))

    var firstColumnValue = new ColumnNameValue("col1",  new Integer(1))
    var secondColumnValue = new ColumnNameValue("col2", "test_value")

    var streamData = Seq(firstColumnValue, secondColumnValue)

    try {
      var total: Integer = 10000
      var i : Integer = 0

      while(i<total) {

        println(s">> Inserting element: $i")

        firstColumnValue = new ColumnNameValue("col1", i)
        secondColumnValue = new ColumnNameValue("col2", s"test-$i")
        streamData = Seq(firstColumnValue, secondColumnValue)

        if (topicName!=null){
           stratioStreamingAPI.insertData(streamName, streamData, topicName)
        } else {
          stratioStreamingAPI.insertData(streamName, streamData)
        }


        if(i % 10 == 0) {
          Thread.sleep(250)
        }


        i = i + 1

      }
    } catch {
      case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }
  }

  def insertData(): Unit = {

    //var firstColumnValue = new ColumnNameValue("name", "test")
    //var secondColumnValue = new ColumnNameValue("data1", new Integer(1))
    //var thirdColumnValue = new ColumnNameValue("data2", new Double(2))

    var firstColumnValue = new ColumnNameValue("col1",  new Integer(1))
    var secondColumnValue = new ColumnNameValue("col2", "test_value")

    var streamData = Seq(firstColumnValue, secondColumnValue)

    try {
      var i: Integer = 1

     // stratioStreamingAPI.listenStream(streamName)
      stratioStreamingAPI.insertData(streamName, streamData)

      /*
      while(true) {
        println(s">> Inserting element: $i")
        firstColumnValue = new ColumnNameValue("name", s"test-$i")
        secondColumnValue = new ColumnNameValue("data1", new Integer(i))
        thirdColumnValue = new ColumnNameValue("data2", new Double(i.toDouble))
        streamData = Seq(firstColumnValue, secondColumnValue, thirdColumnValue)

        stratioStreamingAPI.insertData(streamName, streamData)

        if(i % 10 == 0) {
          i = 1
          Thread.sleep(500)
        } else {
          i = i + 1
        }
      }*/
    } catch {
      case ssEx: StratioStreamingException => println(ssEx.printStackTrace())
    }
  }
}
