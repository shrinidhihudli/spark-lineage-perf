/**
 * Created by shrinidhihudli on 2/9/15.
 *
 * -- This script covers distinct and union.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *     as (user, action, timespent, query_term, ip_addr, timestamp,
 *         estimated_revenue, page_info, page_links);
 * B = foreach A generate user;
 * C = distinct B parallel $PARALLEL;
 * alpha = load '$HDFS_ROOT/widerow' using PigStorage('\u0001');
 * beta = foreach alpha generate $0 as name;
 * gamma = distinct beta parallel $PARALLEL;
 * D = union C, gamma;
 * E = distinct D parallel $PARALLEL;
 * store E into '$PIGMIX_OUTPUT/L11out';
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object L11 {
  def run(outputPath: String) {

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

    val B = A.map(_._1)

    val C = B.distinct(properties.getProperty("PARALLEL").toInt)

    val alpha = sc.textFile(widerowPath)

    val beta = alpha.map(x => x.split("\u0001")(0))

    val gamma = beta.distinct(properties.getProperty("PARALLEL").toInt)

    val D = C.union(gamma)

    val E = D.distinct(properties.getProperty("PARALLEL").toInt).sortBy(identity)

    E.saveAsTextFile(outputPath)

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
