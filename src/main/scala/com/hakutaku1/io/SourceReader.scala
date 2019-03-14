package com.hakutaku1.io

package workflow.io


import com.hakutaku1.model.jobs.workflow.model.Source
import com.hakutaku1.service.workflow.Result
import org.apache.spark.sql.{DataFrame, SparkSession}
import concurrent.ExecutionContext.Implicits.global


import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

/**
  * Data source reader
  * @since 1.0
  * @auth Aaron Zhao
  */
class SourceReader{


  implicit def ts2long(ts:java.sql.Timestamp):Long = ts.getTime

  /**
    *Read parquet files and register as temporary table.
    * @param spark
    * @param path
    * @param sl
    * @return result with audit items.
    */
  def parquet(spark:SparkSession, srcDF: ()=> DataFrame, sl:Source[_]):Future[Result[Nothing]]={
    val p = Promise[Result[Nothing]]()
    Future {
      val spkTry = Try(srcDF().createOrReplaceTempView(sl.mapTable))
      spkTry match {
        case Failure(e) => p failure e
        case Success(_) => {
          val msg = s"${sl.mapTable} is successfully registered."
          p success Result(status = true,msg=msg)
        }
      }
    }
    p.future

  }

}




