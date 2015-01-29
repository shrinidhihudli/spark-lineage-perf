/**
 * Created by shrinidhihudli on 1/28/15.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object Preliminary {
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
    val count = jointRDD.count()
    var duration2 = System.currentTimeMillis() - start

    println("Number of rows of joint dataset: " + count)
    println("Total time taken for join: " + duration1/1000.0 + " s")
    println("Total time taken for count: " + duration2/1000.0 + " s")

  }

  def loadPropertiesFile():Properties = {
    val properties = new Properties()
    val propFile = new FileInputStream("app.properties")
    properties.load(propFile)
    propFile.close()
    properties
  }
}
