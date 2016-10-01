package gr.ionio.informatics.SparkDatasourcePerf

/**
  * Created by vic on 19/9/2016.
  */
abstract class Datasource(val path: String)

case class Json(override val path: String) extends Datasource(path)
case class Parquet(override val path: String) extends Datasource(path)
case class Postgres(override val path: String, val db: String, val table: String) extends Datasource(path)