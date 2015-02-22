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
  def run(sc: SparkContext, pigMixPath: String, outputPath: String): Long = {

    val properties: Properties = SparkMixUtils.loadPropertiesFile()

    val pageViewsPath = pigMixPath + "page_views_sorted/"

    val start = System.currentTimeMillis()

    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4), SparkMixUtils.safeSplit(x, "\u0001", 5),
      SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, x._2, x._7, x._3))

    val C = B.groupBy(_._1)

    val D = C.map(x => (x._1, x._2.map(y => y._2.toSet.size), x._2.map(y => y._3.toSet).map(y => y.sum),
      x._2.map(y => (y._4.toSet, y._4.toSet.size)).map(y => y._1.sum / y._2)))

    val end = System.currentTimeMillis()

    D.saveAsTextFile(outputPath)

    return (end - start)

  }
}
