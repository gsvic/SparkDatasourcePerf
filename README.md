# SparkDatasourcePerf

##Description
SparkDatasourcePerf is a simple tool for measuring time performance of Spark SQL data loading operations. One can easily deploy an experiment, run it and get the performance results (execution time, input data size, number of rows, number of columns etc)

##Sample Usage

```scala
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
```

##Acknowlegements
This project developed as part of the assignment for the Big Data Management Technologies course at Ionian University. The course [website](http://users.ionio.gr/~dtsouma/bigdata-fall2016.html) 
