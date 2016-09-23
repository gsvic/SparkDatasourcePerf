import gr.ionio.informatics.SparkDatasourcePerf.Experiment

/**
  * Created by vic on 23/9/2016.
  */
object Main extends App{
  val sampleExps =
    """parquet,hdfs://master:9000/tpch/tpch_parquet_1/customer
      |parquet,hdfs://master:9000/tpch/tpch_parquet_5/customer
      |json,hdfs://master:9000/tpch/tpch_json_5/customer
      |json,hdfs://master:9000/tpch/tpch_json_1/customer
      |parquet,hdfs://master:9000/tpch/tpch_parquet_20/customer
      |parquet,hdfs://master:9000/tpch/tpch_parquet_50/customer""".stripMargin

  //(new Move(Parquet("hdfs://master:9000/tpch/tpch_parquet_50/customer"), Json("hdfs://master:9000/tpch/tpch_json_50/customer"))).run()
  Experiment.runExperimentsFromFile("json,hdfs://master:9000/tpch/tpch_json_50/customer")
}