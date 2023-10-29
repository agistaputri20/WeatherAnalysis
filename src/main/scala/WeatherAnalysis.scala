import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}

object WeatherAnalysis {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/WB/"
    if (args.size < 2) {
      logFile = "src/main/resources/output.txt"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("WeatherAnalysis-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)


    lc.setCaptureLineage(true)

    //job

    val lines = lc.textFile(logFile, 1)

    var newInput = List[String]()
    var temporaryData = List[String]()

    def push_to_array(element: ((String, String), Float)): Unit = {
      val ((str1, str2), float) = element
      val concatenatedElement = s"$str1,$str2,$float"
      temporaryData = temporaryData :+ concatenatedElement
    }

    val split = lines.flatMap { s =>
      val tokens = s.split(",")

      // finds the state for a zipcode
      val state = zipToState(tokens(0))

      val date = tokens(1)

      // gets snow value and converts it into millimeter
      val snow = convert_to_mm(tokens(2))

      //gets year
      val year = date.substring(date.lastIndexOf("/"))

      // gets month / date
      val monthdate = date.substring(0, date.lastIndexOf("/") - 1)

      List[((String, String), Float)](
        ((state, monthdate), snow),
        ((state, year), snow)
      ).iterator

    }

    val deltaNotSnow = split.groupByKey().map { s =>
      val delta = s._2.max - s._2.min
      (s._1, delta)
    }.filter(s => WeatherAnalysis.successful(s._2))

    val deltaSnow = split.groupByKey().map { s =>
      val delta = s._2.max - s._2.min
      (s._1, delta)
    }.filter(s => WeatherAnalysis.failure(s._2))

    val outputNotSnow = deltaNotSnow.collect()
    print("This is the list of deltaSnow which under the number of failure:\n")
    outputNotSnow.foreach(push_to_array)
    outputNotSnow.foreach(println)
    newInput.foreach(println)

    // re-train the successful dataset

    val outputSnow = deltaSnow.collect()
    print("This is the list of deltaSnow which exceed the number of failure:\n")
    outputSnow.foreach(println)

    // foreach generate new input and save into list

    lc.setCaptureLineage(false)
    Thread.sleep(1000)

    var list = List[Float]()
    for (o <- outputSnow) {
      list = o._2 :: list

    }

    var linRdd = deltaSnow.getLineage()
//    linRdd.collect
    linRdd = linRdd.filter { l => list.contains(l) }
    linRdd = linRdd.goBackAll()
    linRdd.show(true)



    println("Job's DONE!")
    sc.stop()


  }

  def convert_to_mm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit match {
      case "mm" => v
      case _ => v * 304.8f
    }
  }

  def successful(record:Float): Boolean ={
    record <= 158f
  }

  def failure(record: Float): Boolean = {
    record > 158f
  }

  def zipToState(str : String):String = {
    (str.toInt % 50).toString
  }

}