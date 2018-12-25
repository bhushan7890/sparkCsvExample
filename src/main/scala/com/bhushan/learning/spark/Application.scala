package com.bhushan.learning.spark

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Try

object Application extends App {


  override def main(args: Array[String]): Unit = {

    FileUtils.deleteDirectory(new File("src/main/resources/quar"))

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = sparkSession.read
        .format("csv")
      .option("header", "true")
      .option("inferSchema",false)
      .option("badRecordsPath", "/Users/purib/gitLearning/sparkhomework/src/main/resources/quar.txt")
      .csv("src/main/resources/emp.txt")
      .cache()


    df.printSchema()

    val column = df.columns
    //get the schema from the 1st row
    val fieldType: ListBuffer[StructField] = new ListBuffer[StructField]
    val fieldTypeV2: Array[String] =new Array[String](5)
    val firstRow: Seq[Any] = df.first().toSeq
    for(i<-0 until column.length){
      checkString(firstRow(i).toString) match {
        case "int" => {
          fieldTypeV2(i)="int"
          fieldType+=StructField(column(i).trim,IntegerType,false)
        }
        case "double" => {
          fieldTypeV2(i)="double"
          fieldType+=StructField(column(i).trim,DoubleType,false)
        }
        case "string" => {
          fieldTypeV2(i)="string"
          fieldType+=StructField(column(i).trim,StringType,false)
        }
      }
    }


//      .map(row=>{
//      for(i <- 0 until column.length){
//        row.get(i) match {
//          case x: String => fieldType+=StructField(column(i).trim,StringType,false)
//          case x: Int => fieldType+=StructField(column(i).trim,IntegerType,false)
//          case x: Double => fieldType+=StructField(column(i).trim,DoubleType,false)
//          case x => fieldType+=StructField(column(i).trim,StringType,false)
//        }
//      }
//      fieldType
//    })(encoder)

    val schema: StructType = StructType(StructField("Error",StringType,false)+:fieldType.toList)

//    val newSchemaDf = sparkSession.createDataFrame(df.rdd,schema)
//    print(newSchemaDf.show(false))
    var errorRows: List[(String,Row)] = List()
    val cleanedRdd : RDD[Row] = df.rdd.map(row => {
      var newRow = row
      val listRows: ListBuffer[Any] = new ListBuffer[Any]
      try {
        if(row.anyNull){
          newRow = Row.fromSeq("Less number of rows" +: row.toSeq)
        }
        else {
          for (i <- 0 until column.length) {
            val data = row.get(i)
            val cleanData = data.toString.trim.replaceAll("\"", "").replaceAll("\t", "")
            fieldTypeV2(i) match {
              case "int" => listRows += {
                if (cleanData.isEmpty)
                  0
                else
                  cleanData.toInt
              }
              case "double" => listRows += {
                if (cleanData.isEmpty)
                  0.00
                else
                  cleanData.toDouble
              }
              case "string" => listRows += {
                if (cleanData.isEmpty)
                  null
                else
                  cleanData
              }
            }
          }
        }
      } catch {
          case _: Throwable => newRow =  Row.fromSeq("Error in Casting" +: row.toSeq)
      }

      listRows.length==column.length match{
        case true =>{
          Row.fromSeq("No error"+:listRows)
        }
        case false => {
          newRow
        }
      }
    })
    val validRdd = cleanedRdd.filter(_.getString(0).equals("No error"))
    val errorRdd = cleanedRdd.filter(!_.getString(0).equals("No error"))
    val validDF = sparkSession.createDataFrame(validRdd,schema).drop("Error").sort("name")
    validDF.coalesce(1).write
      .option("header",true)
      .mode(SaveMode.Overwrite)
      .csv("src/main/resources/output")
    errorRdd.coalesce(1)
      .saveAsTextFile("src/main/resources/quar")
    print(validDF.show(false))
    print(errorRows.length)
  }

  def checkString(aString: String) :String = {
    var ret: String = ""
    if(isInt(aString)) ret="int"
    else if(isLong(aString)) ret="long"
    else if(isDouble(aString)) ret="double"
    else ret="string"
    ret
  }
  def isShort(aString: String): Boolean = Try(aString.toLong).isSuccess
  def isInt(aString: String): Boolean = Try(aString.toInt).isSuccess
  def isLong(aString: String): Boolean = Try(aString.toLong).isSuccess
  def isDouble(aString: String): Boolean = Try(aString.toDouble).isSuccess
  def isFloat(aString: String): Boolean = Try(aString.toFloat).isSuccess
}
