package com.hakutaku1.service

import com.hakutaku1.model.connectors.{ParquetFileConnector, RedisConnector}
import com.hakutaku1.model.jobs.workflow.model.{Source, SparkJob, Target}
import com.hakutaku1.service.workflow.JobRunner
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success}

/**
  * Application interface
  * @since 1.0
  * @author Aaron Zhao
  */
class Application {

  def out1 = {
    val parqSource = Source(10,"parquet at sample_data folder","intermediate step: read and group"
      ,"Aaron Zhao","_SAMPLE_DATA_",ParquetFileConnector("/tmp/hakutaku1/sample_data"))

    val target = Target("intermediate data result","the intermediate store","Aaron Zhao",1
      ,ParquetFileConnector("/tmp/hakutaku1/page_set_result",None))

    val job = SparkJob("Intermediate job","1.0",1,false,false,5,2,"/tmp/hakutaku1/hakutaku1.sql"
      ,List(parqSource),target,1,"hakutaku1 app",timeout=5)
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("hakutaku1")
      .getOrCreate()

    val jobRunner = new JobRunner(spark,job)
    jobRunner.runWithSpark
  }

  def out2 = {

    val src = Source(10,"intermediate data result","read intermediate step"
      ,"Aaron Zhao","_SAMPLE_DATA_",ParquetFileConnector("/tmp/hakutaku1/page_set_result"))

    val redisTarget =  Target("intermediate data result","the intermediate store","Aaron Zhao",1
      ,RedisConnector("127.0.0.1","6379","redisauth","pageview","profIsFirst"))

    val job = SparkJob("Intermediate job","1.0",1,false,false,5,2,"/tmp/hakutaku1/hakutaku1.sql"
      ,List(src),redisTarget,1,"hakutaku1 app",timeout=5)
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("hakutaku1")
      .getOrCreate()

    val jobRunner = new JobRunner(spark,job)
    jobRunner.runWithSpark

  }

}

object Application{
  def main(args: Array[String]): Unit = {
    val app = new Application
    //Run and write to parquet
    val r1= app.out1
    r1 match {
        // write to Redis
      case Success(_) => app.out2
      case Failure(e)=> throw e
    }
  }
}
