package com.hakutaku1.service

import com.hakutaku1.model.connectors.ParquetFileConnector
import com.hakutaku1.model.jobs.workflow.model.{Source, SparkJob, Target}
import com.hakutaku1.service.workflow.JobRunner
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class SparkJobRunnerTest extends FlatSpec with Matchers{

  "Write item " should " not empty when no new records from source " in {

    val parqSource = Source(10,"parquet at sample_data folder","intermediate step: read and group"
      ,"Aaron Zhao","_SAMPLE_DATA_",ParquetFileConnector("/tmp/hakutaku1/sample_data"))

    val target = Target("intermediate data result","the intermediate store","Aaron Zhao",1
      ,ParquetFileConnector(s"/tmp/hakutaku1/page_set_result",None))

    val job = SparkJob("Intermediate job","1.0",1,false,false,5,2,"/tmp/hakutaku1/hakutaku1.sql"
      ,List(parqSource),target,1,"hakutaku1 app",timeout=5)
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("hakutaku1")
      .getOrCreate()

    val jobRunner = new JobRunner(spark,job)
    val r = jobRunner.runWithSpark.get
    println(r)
    println("test done")
  }
}
