package gr.ionio.informatics.SparkDatasourcePerf

import java.io.BufferedWriter
import java.nio.file.{StandardOpenOption, Paths, Files}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkConf
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
    csr=
      s"${result.get("datasource").get},${result.get("columns").get},${result.get("rows").get},${result.get("size").get},${result.get("time").get}"

    csr
  }

  def writeToDisk() = {
    if (!Files.exists(Paths.get("results.txt")))
      Files.createFile(Paths.get("results.txt"))
    Files.write(Paths.get("results.txt"), s"${this.getCommaSeperatedResult()}\n".getBytes(), StandardOpenOption.APPEND)
  }
  val result: HashMap[String, Any] =  new HashMap[String, Any]()
}

/**
  * A load [[Experiment]] profiles the loading stage for various data-sources.
  *
  * @param ds: The [[Datasource]], e.g. Parquet("hdfs://host:9000/path")
  */

case class Load(ds: Datasource) extends Experiment{

  override def run(): Experiment = {
    var sizeMb = -1.0
    val start = System.currentTimeMillis()
    val df = ds match {
      case Json(path) => {
        val df = Experiment.sqlContext.read.json(path)
        sizeMb = Experiment.fs.getContentSummary(new Path(ds.path)).getSpaceConsumed / 1000000.0
        df
      }
      case Parquet(path) => {
        sizeMb = Experiment.fs.getContentSummary(new Path(ds.path)).getSpaceConsumed / 1000000.0
        val df = Experiment.sqlContext.read.parquet(path)
        df
      }
      case JDBC(host, db, table) => {
        val jdbcURL= s"jdbc:postgresql://${host}:5432/${db}?user=postgres&password=postgres"
        val props = new Properties()
        props.setProperty("driver", "org.postgresql.Driver")
        val df = Experiment.sqlContext.read.jdbc(jdbcURL, table, props)
        df
      }
    }

    val end = (System.currentTimeMillis() - start) / 1000.0

    result.put("time", end)
    result.put("size", sizeMb)
    result.put("datasource", ds)
    result.put("rows", df.count)
    result.put("columns", df.columns.size)

    this
  }
}

/**
  * A move [[Experiment]] profiles a move operation from a datasource to another
  *
  * @param src: The source [[Datasource]], e.g. Parquet("hdfs://host:9000/path.parquet")
  * @param dst: The destination [[Datasource]], e.g. Json("hdfs://host:9000/path.json")
  */

case class Move(src: Datasource, dst: Datasource) extends Experiment{
  val srcDF = src match{
    case Json(path) => Experiment.sqlContext.read.json(path)
    case Parquet(path) => Experiment.sqlContext.read.parquet(path)
  }
  if (Experiment.fs.exists(new Path(dst.path))){
    Experiment.fs.delete(new Path(dst.path), true)
  }
  override def run(): Move ={
    dst match{
      case Json(path) => srcDF.write.json(path)
      case Parquet(path) => srcDF.write.parquet(path)
    }
    this
  }
}

/**
  * A "helper" class, providing various useful methods
  */

object Experiment{
  val sconf = new SparkConf().setJars(Seq("postgres.jar"))
  val sqlContext = SparkSession.builder()
    .config(sconf)
    .master("spark://master:7077")
    .config("spark.eventLog.enabled", "true")
    .appName("Big Data Project")
    .getOrCreate()

  private lazy val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://master:9000")

  lazy val fs = FileSystem.get(Experiment.conf)

  /**
    *
    * @param experiments: A line-seperated string. Each line contains the comma-seperated experiment parameters. For
    *                   example: "parquet,hdfs://master:9000/tpch/tpch_parquet_5/customer" defines an experiment in
    *                   which a parquet file is being loaded located at hdfs://master:9000/tpch/tpch_parquet_5/customer
    */
  def runExperimentsFromFile(experiments: String): Unit = {
    val exps = experiments.split("\n").map{l =>
      val params = l.split(",")
      val source = params(0)
      val path = params(1)
      val exp = source match{
        case "parquet" => Parquet(path)
        case "json" => Json(path)
        case "jdbc" => {
          val host = path
          val db = params(2)
          val table = params(3)
          JDBC(host, db, table)
        }
      }
      Load(exp).run()
    }
    exps.foreach(_.run)
    exps.foreach(experiment => println(experiment.getCommaSeperatedResult()))
  }
}

