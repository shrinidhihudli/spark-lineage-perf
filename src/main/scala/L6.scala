/**
 * Created by shrinidhihudli on 2/7/15.
 *
 * -- This script covers the case where the group by key is a significant
 * -- percentage of the row.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *    as (user, action, timespent, query_term, ip_addr, timestamp,
 *        estimated_revenue, page_info, page_links);
 * B = foreach A generate user, action, (int)timespent as timespent, query_term, ip_addr, timestamp;
 * C = group B by (user, query_term, ip_addr, timestamp) parallel $PARALLEL;
 * D = foreach C generate flatten(group), SUM(B.timespent);
 * store D into '$PIGMIX_OUTPUT/L6out';
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object L6 {
  def run(sc: SparkContext,outputPath: String) {

    val properties: Properties = loadPropertiesFile()

    val pigMixPath = properties.getProperty("pigMix")
    val pageViewsPath = pigMixPath + "page_views/"
    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (safeSplit(x,"\u0001",0), safeSplit(x,"\u0001",1), safeSplit(x,"\u0001",2),
      safeSplit(x,"\u0001",3), safeSplit(x,"\u0001",4), safeSplit(x,"\u0001",5), safeSplit(x,"\u0001",6),
      createMap(safeSplit(x,"\u0001",7)), createBag(safeSplit(x,"\u0001",8))))

    val B = A.map(x => (x._1,x._2,safeInt(x._3),x._4,x._5,x._6))

    val C = B.groupBy(x => (x._1,x._4,x._5,x._6)) //TODO add $PARALLEL

    val D = C.map(x => (x._1,x._2.reduce((a,b) => (a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4,a._5+b._5,a._6+b._6))))
      .map(x => (x._1._1,x._1._2,x._1._3,x._1._4,x._2._3)).sortBy(_._1)

    D.saveAsTextFile(outputPath)

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
