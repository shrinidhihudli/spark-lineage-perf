/**
 * Created by shrinidhihudli on 2/7/15.
 *
 * -- This script covers having a nested plan with splits.
 * register '/usr/local/Cellar/pig/0.14.0/test/perf/pigmix/pigmix.jar'
 * A = load '/user/pig/tests/data/pigmix/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *     as (user, action, timespent, query_term, ip_addr, timestamp,
 *        estimated_revenue, page_info, page_links);
 * B = foreach A generate user, timestamp;
 * C = group B by user parallel $PARALLEL;
 * D = foreach C {
 *     morning = filter B by timestamp < 43200;
 *     afternoon = filter B by timestamp >= 43200;
 *     generate group, COUNT(morning), COUNT(afternoon);
 * }
 * store D into '$PIGMIX_OUTPUT/L7out';
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object L7 {
  def main(args: Array[String]) {

    val properties: Properties = loadPropertiesFile()

    val pigMixPath = properties.getProperty("pigMix")
    val pageViewsPath = pigMixPath + "page_views/"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (safeSplit(x,"\u0001",0), safeSplit(x,"\u0001",1), safeSplit(x,"\u0001",2),
      safeSplit(x,"\u0001",3), safeSplit(x,"\u0001",4), safeSplit(x,"\u0001",5), safeSplit(x,"\u0001",6),
      createMap(safeSplit(x,"\u0001",7)), createBag(safeSplit(x,"\u0001",8))))

    val B = A.map(x => (x._1,safeInt(x._6)))

    val C = B.groupBy(_._1) //TODO add $PARALLEL

    val D = C.mapValues(x => x.map( y => if (y._2 < 43200) "morning" else "afternoon"))
      .map(x => (x._1,x._2.groupBy(identity))).map(x => (x._1,x._2.mapValues(x => x.size).map(identity))).sortBy(_._1)

    D.saveAsTextFile("output/L7out")

  }

  def safeInt(string: String):Int = {
    if (string == "")
      0
    else
      string.toInt
  }

  def safeSplit(string: String, delim: String, int: Int):String = {
    val split = string.split(delim)
    if (split.size > int)
      split(int)
    else
      ""
  }

  def createMap(mapString:String):Map[String, String] = {
    val map = mapString.split("\u0003").map( x => (x.split("\u0004")(0),x.split("\u0004")(1))).toMap
    map
  }

  def createBag(bagString:String):Array[Map[String, String]] = {
    val bag = bagString.split("\u0002").map(x => createMap(x))
    bag
  }

  def loadPropertiesFile():Properties = {
    val properties = new Properties()
    val propFile = new FileInputStream("app.properties")
    properties.load(propFile)
    propFile.close()
    properties
  }
}
