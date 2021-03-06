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
  def run(sc: SparkContext, pigMixPath: String, outputPath: String): Long = {

    val properties: Properties = SparkMixUtils.loadPropertiesFile()

    val pageViewsPath = pigMixPath + "page_views/"

    val start = System.currentTimeMillis()

    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4), SparkMixUtils.safeSplit(x, "\u0001", 5),
      SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, x._2, SparkMixUtils.safeInt(x._3), x._4, x._5, x._6))

    val C = B.groupBy(x => (x._1, x._4, x._5, x._6)) //TODO add $PARALLEL

    val D = C.map(x => (x._1,
      x._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))))
      .map(x => (x._1._1, x._1._2, x._1._3, x._1._4, x._2._3))

    val end = System.currentTimeMillis()

    D.saveAsTextFile(outputPath)

    return (end - start)

  }
}
