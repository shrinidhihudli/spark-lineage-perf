import java.io.{File, PrintWriter, FileInputStream}
import java.util.Properties

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shrinidhihudli on 2/10/15.
 */

object SparkMix {

  def main (args: Array[String]) {

    val properties = SparkMixUtils.loadPropertiesFile()
    val datasize = args(0)
    val pigmixPath = properties.getProperty("pigMix") + "pigmix_" + datasize + "/"
    val outputRoot = properties.getProperty("output") + "pigmix_" + datasize + "/"

    new File(outputRoot).mkdir()

    val conf = new SparkConf().setAppName("SparkMix").setMaster("local")
    val sc = new SparkContext(conf)

    val start = System.currentTimeMillis()

    L1.run(sc,pigmixPath,outputRoot + "L1out")
    L2.run(sc,pigmixPath,outputRoot + "L2out")
    L3.run(sc,pigmixPath,outputRoot + "L3out")
    L4.run(sc,pigmixPath,outputRoot + "L4out")
    L5.run(sc,pigmixPath,outputRoot + "L5out")
    L6.run(sc,pigmixPath,outputRoot + "L6out")
    L7.run(sc,pigmixPath,outputRoot + "L7out")
    L8.run(sc,pigmixPath,outputRoot + "L8out")
    L9.run(sc,pigmixPath,outputRoot + "L9out")
    L10.run(sc,pigmixPath,outputRoot + "L10out")
    L11.run(sc,pigmixPath,outputRoot + "L11out")
    L12.run(sc,pigmixPath,outputRoot + "L12out")
    L13.run(sc,pigmixPath,outputRoot + "L13out")
    L14.run(sc,pigmixPath,outputRoot + "L14out")
    L15.run(sc,pigmixPath,outputRoot + "L15out")
    L16.run(sc,pigmixPath,outputRoot + "L16out")

    val stop = System.currentTimeMillis()

    val pw = new PrintWriter(new File(outputRoot + "log/time.txt"))
    pw.write(((stop - start)/1000.0).toString + "s")
  }
}
