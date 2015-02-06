/**
 * Created by shrinidhihudli on 1/29/15.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object YahooMusicAverageRating {
  def main(args: Array[String]) {

    val properties: Properties = loadPropertiesFile()
    val ratingsFile = properties.getProperty("ratingsFile")
    val tracksFile =  properties.getProperty("tracksFile");

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    var ratings = sc.textFile(ratingsFile)
    var tracks = sc.textFile(tracksFile)
    ratings = ratings.filter(line => !line.contains("|"))
    val ratingsPair = ratings.map(x => (x.split('\t')(0),x))
    val tracksPair = tracks.map(x => (x.split('|')(0),x))

    var start = System.currentTimeMillis()
    val jointRDD = tracksPair.join(ratingsPair)
    val duration1 = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    var freqs = jointRDD.map(x => (x._1,x._2._2.split('\t')(1))).
      mapValues(x => (x.toInt,1)).
      reduceByKey((x,y) => (x._1 + y._1,x._2 + y._2)).
      map{ case (key, value) => (key, value._1 / value._2.toFloat) }.
      sortBy(-_._2)
    val duration2 = System.currentTimeMillis() - start

    freqs.saveAsTextFile("data/trainratings")

    println("Total time taken for join: " + duration1/1000.0 + " s")
    println("Total time taken for calculating sorted average rating per song: " + duration2/1000.0 + " s")

  }

  def loadPropertiesFile():Properties = {
    val properties = new Properties()
    val propFile = new FileInputStream("app.properties")
    properties.load(propFile)
    propFile.close()
    properties
  }
}

