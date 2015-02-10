/**
 * Created by shrinidhihudli on 2/9/15.
 *
 * -- This script covers multi-store queries.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *    as (user, action, timespent, query_term, ip_addr, timestamp,
 *        estimated_revenue, page_info, page_links);
 * B = foreach A generate user, action, (int)timespent as timespent, query_term,
 *     (double)estimated_revenue as estimated_revenue;
 * split B into C if user is not null, alpha if user is null;
 * split C into D if query_term is not null, aleph if query_term is null;
 * E = group D by user parallel $PARALLEL;
 * F = foreach E generate group, MAX(D.estimated_revenue);
 * store F into '$PIGMIX_OUTPUT/highest_value_page_per_user';
 * beta = group alpha by query_term parallel $PARALLEL;
 * gamma = foreach beta generate group, SUM(alpha.timespent);
 * store gamma into '$PIGMIX_OUTPUT/total_timespent_per_term';
 * beth = group aleph by action parallel $PARALLEL;
 * gimel = foreach beth generate group, COUNT(aleph);
 * store gimel into '$PIGMIX_OUTPUT/queries_per_action';
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object L12 {
  def main(args: Array[String]) {

    val properties: Properties = loadPropertiesFile()

    val pigMixPath = properties.getProperty("pigMix")
    val pageViewsPath = pigMixPath + "page_views/"
    val widerowPath = pigMixPath + "widerow/"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (safeSplit(x,"\u0001",0), safeSplit(x,"\u0001",1), safeSplit(x,"\u0001",2),
      safeSplit(x,"\u0001",3), safeSplit(x,"\u0001",4), safeSplit(x,"\u0001",5), safeSplit(x,"\u0001",6),
      createMap(safeSplit(x,"\u0001",7)), createBag(safeSplit(x,"\u0001",8))))

    val B = A.map(x => (x._1,x._2,safeInt(x._3),x._4,safeDouble(x._7)))

    val C = B.filter(_._1 != null)

    val alpha = B.subtract(C)

    val D = C.filter(_._4 != null)

    val aleph = C.subtract(D)

    val E = D.groupBy(_._1)

    val F = E.map(x => (x._1,x._2.reduce((a,b) => ("","",0,"",Math.max(a._5,b._5)))))

    F.saveAsTextFile("output/L12out/highest_value_page_per_user")

    val beta = alpha.groupBy(_._4)

    val gamma = beta.map(x => (x._1,x._2.reduce((a,b) => ("","",a._3 + b._3,"",0))))

    gamma.saveAsTextFile("output/L12out/total_timespent_per_term")

    val beth = aleph.groupBy(_._2)

    val gimel = beth.map(x => (x._1,x._2.size))

    gimel.saveAsTextFile("output/L12out/queries_per_action")

  }

  def safeInt(string: String):Int = {
    if (string == "")
      0
    else
      string.toInt
  }

  def safeDouble(string: String):Double = {
    if (string == "")
      0
    else
      string.toDouble
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
