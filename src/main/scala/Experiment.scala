import gr.ionio.informatics.SparkDatasourcePerf.{Parquet, Json, Datasource}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.HashMap

/**
  * An abstract definition of an experiment
  * */

abstract class Experiment(){
  def run(): Experiment
  def getCommaSeperatedResult(): String = {
    var csr = ""
    val iter = result.values.iterator
    iter.foreach(x => {
      if (iter.hasNext)
        csr += s"${x}, "
      else
        csr += x
    })
    csr
  }
  val result: HashMap[String, Any] =  new HashMap[String, Any]()
}

/**
  * A load [[Experiment]] profiles the loading stage for various data-sources.
  * @param ds: The [[Datasource]], e.g. Parquet("hdfs://host:9000/path")
  */

case class Load(ds: Datasource) extends Experiment{

  override def run(): Experiment = {
    val start = System.currentTimeMillis()
    val df = ds match {
      case Json(path) => Experiment.sqlContext.read.json(path)
      case Parquet(path) => Experiment.sqlContext.read.parquet(path)
    }
    val size = FileSystem.get(Experiment.conf).getContentSummary(new Path(ds.path)).getSpaceConsumed / 1000000.0
    val end = (System.currentTimeMillis() - start) / 1000.0

    result.put("time", end)
    result.put("size", size)
    result.put("datasource", ds)
    result.put("rows", df.count)
    result.put("columns", df.columns.size)

    this
  }

}

/**
  * A "helper" class, providing various useful methods
  */

object Experiment{
  val sqlContext = SparkSession.builder()
    .master("spark://master:7077")
    .config("spark.eventLog.enabled", "true").appName("Big Data Project").getOrCreate()

  lazy val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://master:9000")

  /**
    *
    * @param experiments: A line-seperated string. Each line contains the comma-seperated experiment parameters. For
    *                   example: "parquet,hdfs://master:9000/tpch/tpch_parquet_5/customer" defines an experiment in
    *                   which a parquet file is being loaded located at hdfs://master:9000/tpch/tpch_parquet_5/customer
    */
  def runExperimentsFromFile(experiments: String): Unit = {
    experiments.split("\n").foreach{l =>
      val params = l.split(",")
      val source = params(0)
      val path = params(1)
      val exp = source match{
        case "parquet" => Parquet(path)
        case "json" => Json(path)
      }

      println(Load(exp).run().result)

    }
  }
}



object Main extends App{
  val sampleExps =
    """parquet,hdfs://master:9000/tpch/tpch_parquet_5/customer
      |parquet,hdfs://master:9000/tpch/tpch_parquet_20/customer
      |parquet,hdfs://master:9000/tpch/tpch_parquet_50/customer""".stripMargin

  Experiment.runExperimentsFromFile(sampleExps)
}