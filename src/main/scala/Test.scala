import gr.ionio.informatics.SparkDatasourcePerf._
import gr.ionio.informatics.SparkDatasourcePerf.Experiment
import org.apache.spark.sql.SparkSession

object Main extends App{
  val spark = new SparkSession()
  Experiment.init(spark) //Initialization

  val loadFromPostgres = new Load(Postgres("192.168.5.172", "1G", "customer"))
  val loadJsonFile = new Load(Json("1"))

  println(loadFromPostgres.run.getCommaSeperatedResult)
  println(loadJsonFile.run.getCommaSeperatedResult)
}