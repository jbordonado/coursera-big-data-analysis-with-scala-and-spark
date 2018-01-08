package timeusage

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {
  val columnsName: List[String] = List(
    "t010101",
    "t010102",
    "t010199",
    "t010201",
    "t010299",
    "t020699",
    "t020701",
    "t020799",
    "t030101",
    "t030102",
    "t030103",
    "t030104",
    "t030105",
    "t030108",
    "t030109",
    "t050189",
    "t050201",
    "t050202",
    "t050203",
    "t180104",
    "t180305",
    "t180599",
    "t180502",
    "t181181"
  )

  test("classifiedColumns") {
    val (primaryNeeds, work, other) = TimeUsage.classifiedColumns(columnsName)

    println("-------------- Primary Needs --------------")
    primaryNeeds.foreach(println)
    println("-------------- Work --------------")
    work.foreach(println)
    println("-------------- Other --------------")
    other.foreach(println)

    assert(primaryNeeds.size === 5 + 7 + 0 + 1 + 1) // “t01”, “t03”, “t11”, “t1801” and “t1803”
    assert(work.size === 4 + 2) // “t05” and “t1805”
    assert(other.size === 3 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 0 + 1) // “t02”, “t04”, “t06”, “t07”, “t08”, “t09”, “t10”, “t12”, “t13”, “t14”, “t15”, “t16” and “t18” (those which are not part of the previous groups only).
  }
}
