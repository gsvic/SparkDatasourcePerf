package gr.ionio.informatics.SparkDatasourcePerf

import org.apache.spark.sql.DataFrame

/**
  * Created by vic on 19/9/2016.
  */
abstract class Datasource(val path: String)

case class Json(override val path: String) extends Datasource(path)
case class Parquet(override val path: String) extends Datasource(path)
case class JDBC(override val path: String, val db: String, val table: String) extends Datasource(path)