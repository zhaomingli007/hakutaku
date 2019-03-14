package com.hakutaku1.io

package workflow.io

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.hakutaku1.model.connectors.RedisConnector
import com.hakutaku1.model.jobs.workflow.model.{Source, SparkJob, Target}
import com.hakutaku1.service.workflow.Result
import org.apache.spark.sql.{DataFrameWriter, Row, SaveMode, SparkSession}

import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}
import concurrent.ExecutionContext.Implicits.global


/**
  * Target file writer
  * @since 1.0
  * @auth Aaron Zhao
  */
class TargetWriter(job: SparkJob) {

  val _RES_TO_SAVE_ = "_RES_TO_SAVE_"


  /**
    * Write to Redis
    */

  def redis(preSpark:SparkSession,host:String,port:String,auth:String,tbl:String,key:String):Future[Result[String]]={
    preSpark.stop()
    val spark = SparkSession
      .builder()
      .appName("redis")
      .master("local[*]")
      .config("spark.redis.host", host)
      .config("spark.redis.port", port)
      .config("spark.redis.auth", auth)
      .getOrCreate()


    val p = Promise[Result[String]]()

    Future {
      val spkTry = Try {
        val writeDF = spark.sql(s"SELECT * FROM ${_RES_TO_SAVE_}")
        writeDF
          .write
          .format("org.apache.spark.sql.redis")
          .option("table", tbl)
          .option("key.column", key)
          .save()
      }
      spkTry match {
        case Failure(e) => p failure e
        case Success(v) => {
          val msg = s"data set had been stored at ${tbl} successfully."
          p success Result(true, msg,None)
        }
      }
    }
    p.future
  }

  /**
    * Writer to parquet file
    * @param spark spark session
    * @param path the base path write to
    * @param partitionCol partition column
    * @param target the target
    * @param saveMode save mode
    * @return
    */
  def parquet(spark: SparkSession, path: String, partitionCol: Option[String], target: Target, saveMode: String) = {
    val p = s"$path/${getPrefixPath(target)}"
    lazy val doWrite = (cnt: Long, wr: DataFrameWriter[Row]) => {
      if (cnt > 0) {
        wr.mode(saveMode).parquet(p)
        Some(p)
      } else None
    }
    write(spark, partitionCol, target, doWrite)
  }

  def getPrefixPath(target: Target) = if (target.isAddTimeStamp)
    LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMddHHmm"))
  else ""


  private[this] def write(spark: SparkSession, partitionCol: Option[String], target: Target
                          , doWrite: (Long, DataFrameWriter[Row]) => Option[String]): Future[Result[String]] = {
    val p = Promise[Result[String]]()
    Future {
      val spkTry = Try {
        val writeDF = spark.sql(s"SELECT * FROM ${_RES_TO_SAVE_}")
        val count = writeDF.count
        val dfWriter = partitionCol match {
          case Some(v) => writeDF.repartition(writeDF(v)).write.partitionBy(v)
          case None => writeDF.coalesce(1).write
        }
        doWrite(count, dfWriter)
      }
      spkTry match {
        case Failure(e) => p failure e
        case Success(v) => {
          val msg = s"data set had been stored at ${target.name} successfully."
          p success Result(true, msg, ret = v)
        }
      }
    }
    p.future
  }


  def write(spark: SparkSession, run: () => Future[Result[_]]): Try[Result[_]] = {
    import scala.concurrent.duration._
    val waitRes = Try(Await.result(run(), job.timeout minutes))
    waitRes match {
      case Success(r) => {
        Success(Result(r.status, r.msg, r.ret))
      }
      case Failure(_) => { //Timeout exception will be catch.
        waitRes
      }
    }
  }
}
