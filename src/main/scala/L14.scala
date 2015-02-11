/**
 * Created by shrinidhihudli on 2/9/15.
 *
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views_sorted' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *    as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
 * B = foreach A generate user, estimated_revenue;
 * alpha = load '$HDFS_ROOT/users_sorted' using PigStorage('\u0001') as (name, phone, address, city, state, zip);
 * beta = foreach alpha generate name;
 * C = join B by user, beta by name using 'merge';
 * store C into '$PIGMIX_OUTPUT/L14out';
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object L14 {
  def main(args: Array[String]) {

    val properties: Properties = loadPropertiesFile()

    val pigMixPath = properties.getProperty("pigMix")
    val pageViewsPath = pigMixPath + "page_views_sorted/"
    val usersPath = pigMixPath + "users_sorted/"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val pageViews = sc.textFile(pageViewsPath)
    val users = sc.textFile(usersPath)

    val A = pageViews.map(x => (safeSplit(x,"\u0001",0), safeSplit(x,"\u0001",1), safeSplit(x,"\u0001",2),
      safeSplit(x,"\u0001",3), safeSplit(x,"\u0001",4), safeSplit(x,"\u0001",5), safeSplit(x,"\u0001",6),
      createMap(safeSplit(x,"\u0001",7)), createBag(safeSplit(x,"\u0001",8))))

    val B = A.map(x => (x._1,x._7))

    val alpha = users.map(x => (safeSplit(x,"\u0001",0),safeSplit(x,"\u0001",1),safeSplit(x,"\u0001",2),
      safeSplit(x,"\u0001",3),safeSplit(x,"\u0001",4),safeSplit(x,"\u0001",5)))

    val beta = alpha.map(x => (x._1,x._1))

    val C = B.join(beta,properties.getProperty("PARALLEL").toInt).sortBy(_._1) // merge join unsupported in Spark

    C.saveAsTextFile("output/L14out")

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