import java.io.{File, PrintWriter, FileInputStream}
import java.util.Properties

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shrinidhihudli on 2/10/15.
 */

object SparkMix {

  def main (args: Array[String]) {

    val start = System.currentTimeMillis()

    val outputRoot = "output2"

    new File(outputRoot).mkdir()

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    L1.run(sc,outputRoot + "/L1out")
    L2.run(sc,outputRoot + "/L2out")
    L3.run(sc,outputRoot + "/L3out")
    L4.run(sc,outputRoot + "/L4out")
    L5.run(sc,outputRoot + "/L5out")
    L6.run(sc,outputRoot + "/L6out")
    L7.run(sc,outputRoot + "/L7out")
    L8.run(sc,outputRoot + "/L8out")
    L9.run(sc,outputRoot + "/L9out")
    L10.run(sc,outputRoot + "/L10out")
    L11.run(sc,outputRoot + "/L11out")
    L12.run(sc,outputRoot + "/L12out")
    L13.run(sc,outputRoot + "/L13out")
    L14.run(sc,outputRoot + "/L14out")
    L15.run(sc,outputRoot + "/L15out")
    L16.run(sc,outputRoot + "/L16out")

    val stop = System.currentTimeMillis()

    val pw = new PrintWriter(new File(outputRoot + "/time.txt"))
    pw.write(((stop - start)/1000.0).toString + "s")
  }
}
