/**
 * Created by shrinidhihudli on 2/19/15.
 *
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/widegroupbydata' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *    as (user, action, timespent, query_term, ip_addr, timestamp,
 *         estimated_revenue, page_info, page_links, user_1, action_1, timespent_1, query_term_1, ip_addr_1, timestamp_1,
 *         estimated_revenue_1, page_info_1, page_links_1);
 * B = group A by (user, action, timespent, query_term, ip_addr, timestamp,
 *         estimated_revenue, user_1, action_1, timespent_1, query_term_1, ip_addr_1, timestamp_1,
 *         estimated_revenue_1) parallel $PARALLEL;
 * C = foreach B generate SUM(A.timespent), SUM(A.timespent_1), AVG(A.estimated_revenue), AVG(A.estimated_revenue_1);
 * store C into '$PIGMIX_OUTPUT/L17out';
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object L17 {
  def run(sc: SparkContext, pigMixPath: String, outputPath: String): Long = {

    val properties: Properties = SparkMixUtils.loadPropertiesFile()

    val pageViewsPath = pigMixPath + "page_views_sorted/"

    val start = System.currentTimeMillis()

    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeDouble(SparkMixUtils.safeSplit(x, "\u0001", 2)), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4), SparkMixUtils.safeSplit(x, "\u0001", 5),
      SparkMixUtils.safeDouble(SparkMixUtils.safeSplit(x, "\u0001", 6)),
      SparkMixUtils.safeSplit(x, "\u0001", 7),
      SparkMixUtils.safeSplit(x, "\u0001", 8), SparkMixUtils.safeSplit(x, "\u0001", 9),
      SparkMixUtils.safeSplit(x, "\u0001", 10), SparkMixUtils.safeDouble(SparkMixUtils.safeSplit(x, "\u0001", 11)),
      SparkMixUtils.safeSplit(x, "\u0001", 12), SparkMixUtils.safeSplit(x, "\u0001", 13),
      SparkMixUtils.safeSplit(x, "\u0001", 14), SparkMixUtils.safeDouble(SparkMixUtils.safeSplit(x, "\u0001", 15)),
      SparkMixUtils.safeSplit(x, "\u0001", 16),
      SparkMixUtils.safeSplit(x, "\u0001", 17)))

    val B = A.groupBy(x => (x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._10, x._11, x._12, x._13, x._14, x._15, x._16))

    val C = B.map(x => (x._1, x._2.toList.size,
      x._2.reduce((a, b) => ("", "", a._3 + b._3, "", "", "", a._7 + b._7,
        "", "", "", "", a._12 + b._12, "", "", "", a._16 + b._16, "", ""))))
      .map(x => (x._1, x._3._3, x._3._12, x._3._7 / x._2, x._3._16 / x._2))

    val end = System.currentTimeMillis()

    C.saveAsTextFile(outputPath)

    return (end - start)

  }
}