/**
 * Created by shrinidhihudli on 2/4/15.
 *
 * -- This script tests reading from a map, flattening a bag of maps, and use of bincond.
 *    register $PIGMIX_JAR
 *    A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *        as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
 *    B = foreach A generate user, (int)action as action, (map[])page_info as page_info,
 *        flatten((bag{tuple(map[])})page_links) as page_links;
 *    C = foreach B generate user,
 *        (action == 1 ? page_info#'a' : page_links#'b') as header;
 *    D = group C by user parallel $PARALLEL;
 *    E = foreach D generate group, COUNT(C) as cnt;
 *    store E into '$PIGMIX_OUTPUT/L1out';
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object L1 {
  def run(outputPath: String) {

    val properties: Properties = loadPropertiesFile()

    val pigMixPath = properties.getProperty("pigMix")
    val pageViewsPath = pigMixPath + "page_views/"

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (safeSplit(x,"\u0001",0), safeSplit(x,"\u0001",1), safeSplit(x,"\u0001",2),
      safeSplit(x,"\u0001",3), safeSplit(x,"\u0001",4), safeSplit(x,"\u0001",5), safeSplit(x,"\u0001",6),
      createMap(safeSplit(x,"\u0001",7)), createBag(safeSplit(x,"\u0001",8))))

    val B = A.map(x => (x._1,x._2,x._8,x._9)).flatMap(r => for(v<-r._4) yield(r._1,r._2,r._3,v))

    val C = B.map(x =>  if (x._2 == "1") (x._1,x._3.get("a").toString) else (x._1,x._4.get("b").toString))

    val D = C.groupBy(x => x._1)  //TODO add $PARALLEL

    val E = D.map(x => (x._1,x._2.size)).sortBy(_._1)

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
