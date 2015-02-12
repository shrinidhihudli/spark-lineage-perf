/**
 * Created by shrinidhihudli on 2/7/15.
 *
 * -- This script covers having a nested plan with splits.
 * register '/usr/local/Cellar/pig/0.14.0/test/perf/pigmix/pigmix.jar'
 * A = load '/user/pig/tests/data/pigmix/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *     as (user, action, timespent, query_term, ip_addr, timestamp,
 *        estimated_revenue, page_info, page_links);
 * B = foreach A generate user, timestamp;
 * C = group B by user parallel $PARALLEL;
 * D = foreach C {
 *     morning = filter B by timestamp < 43200;
 *     afternoon = filter B by timestamp >= 43200;
 *     generate group, COUNT(morning), COUNT(afternoon);
 * }
 * store D into '$PIGMIX_OUTPUT/L7out';
 *
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

object L7 {
  def run(sc: SparkContext,pigMixPath: String,outputPath: String) {

    val properties: Properties = SparkMixUtils.loadPropertiesFile()

    val pageViewsPath = pigMixPath + "page_views/"
    val pageViews = sc.textFile(pageViewsPath)

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x,"\u0001",0), SparkMixUtils.safeSplit(x,"\u0001",1),
      SparkMixUtils.safeSplit(x,"\u0001",2), SparkMixUtils.safeSplit(x,"\u0001",3),
      SparkMixUtils.safeSplit(x,"\u0001",4), SparkMixUtils.safeSplit(x,"\u0001",5),
      SparkMixUtils.safeSplit(x,"\u0001",6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x,"\u0001",7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x,"\u0001",8))))

    val B = A.map(x => (x._1,SparkMixUtils.safeInt(x._6)))

    val C = B.groupBy(_._1) //TODO add $PARALLEL

    val D = C.mapValues(x => x.map( y => if (y._2 < 43200) "morning" else "afternoon"))
      .map(x => (x._1,x._2.groupBy(identity))).map(x => (x._1,x._2.mapValues(x => x.size).map(identity))).sortBy(_._1)

    D.saveAsTextFile(outputPath)

  }
}
