/**
 * Created by shrinidhihudli on 2/10/15.
 *
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *     as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
 * B = foreach A generate user, action, estimated_revenue, timespent;
 * C = group B by user parallel $PARALLEL;
 * D = foreach C {
 *     beth = distinct B.action;
 *     rev = distinct B.estimated_revenue;
 *     ts = distinct B.timespent;
 *     generate group, COUNT(beth), SUM(rev), (int)AVG(ts);
 * }
 * store D into '$PIGMIX_OUTPUT/L15out';
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object L15 {
  def run(outputPath: String) {

    val properties: Properties = loadPropertiesFile()

    val pigMixPath = properties.getProperty("pigMix")
    val pageViewsPath = pigMixPath + "page_views_sorted/"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (safeSplit(x,"\u0001",0), safeSplit(x,"\u0001",1), safeSplit(x,"\u0001",2),
      safeSplit(x,"\u0001",3), safeSplit(x,"\u0001",4), safeSplit(x,"\u0001",5), safeSplit(x,"\u0001",6),
      createMap(safeSplit(x,"\u0001",7)), createBag(safeSplit(x,"\u0001",8))))

    val B = A.map(x => (x._1,x._2,x._7,x._3))

    val C = B.groupBy(_._1)

    val D = C.map(x => (x._1,x._2.map(y => y._2.toSet.size),x._2.map(y => y._3.toSet).map(y => y.sum),
      x._2.map(y => (y._4.toSet,y._4.toSet.size)).map(y => y._1.sum/y._2))).sortBy(_._1)

    D.saveAsTextFile(outputPath)

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
