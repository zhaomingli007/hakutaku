package com.hakutaku1.service
package workflow

import com.hakutaku1.io.workflow.io.{SourceReader, TargetWriter}
import com.hakutaku1.model.connectors.{ParquetFileConnector, RedisConnector}
import com.hakutaku1.model.jobs.workflow.model.{Source, SparkJob, Target}

import scala.concurrent.{Future, Promise}
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

/**
  *
  * @param status function execution status.
  * @param msg return message if any.
  * @param ret Nothing if the function has nothing to return, or the actual return value with parameterized type.
  * @tparam T Type of returned value.
  */
case class Result[+T](status:Boolean=true,msg:String="",ret:Option[T]=None)

class JobRunner(spark:SparkSession,job:SparkJob){
  val reader  = new SourceReader()
  val writer =new TargetWriter(job)
  type SourceList = List[Source[_]]

  val _RES_TO_SAVE_ = "_RES_TO_SAVE_"
  case class ConnectorNotFoundException(err:String) extends Exception(err)


  /**
    * Load and register data set from sources as temporary table in Spark, then run spark SQL and finally save as to target.
    * @param sources: list of data sources to be loaded into spark and registered as temporary table.
    * @param target: target where the data will be stored.
    * @return result either success or fail in the future.
    */
  def run():Future[Result[_]] = {
    for {
      sqlFile <- loadSqlFile(job.sqlFile)
      rNr <- readNreg(job.sources)
      runSQL <- runSparkSQL(sqlFile)
      res <- doPersistent(job.target)
    } yield Result(res.status,res.msg,res.ret)
  }

  def runWithSpark= {
    writer.write(spark,run)
  }


  private[this] def readNreg(sources:SourceList):Future[Result[Nothing]]={
    sources match {
      case Nil => Future(Result()) //TODO warn caller?
      case h :: Nil => regEach(h)
      case h :: t => regEach(h).flatMap(_ =>readNreg(t))
    }
  }

  private[this] def regEach(sl:Source[_]):Future[Result[Nothing]] =
    sl.connector match {
      case ParquetFileConnector(path,_) => reader.parquet(spark,()=>spark.read.parquet(path),sl)
      case _ => {
        val p = Promise[Result[Nothing]]()
        Future {
          p failure ConnectorNotFoundException(s"Connector not found for source ${sl.name}")
        }
        p.future
      }
    }

  private[this] def loadSqlFile(file:String):Future[Result[String]] = {
    val p = Promise[Result[String]]()
    Future{
      val res = Try(scala.io.Source.fromFile(file).getLines().mkString("\n"))
      res match {
        case Success(v) => {
          val msg = s"SQL string is loaded from ${file} successfully."
          p success  Result(true,msg ,Some(v))
        }
        case Failure(e) => p failure e
      }
    }
    p future
  }
  private[this] def runSparkSQL(sqlRes:Result[String]):Future[Result[Nothing]] = {
    //TODO magic name _RES_TO_SAVE_ compared to which from user registered with
    val p = Promise[Result[Nothing]]()
    Future {
      val sqlTry = Try(spark.sql(sqlRes.ret.get).createOrReplaceTempView(_RES_TO_SAVE_))
      sqlTry match {
        case Success(_)  => {
          val msg = "sql script run successfully."
          p success Result(true,msg)
        }
        case Failure(e)  => p failure e
      }
    }
    p future
  }

  private[this] def doPersistent(target:Target):Future[Result[String]] ={
    target.connector match {
      case ParquetFileConnector(path,partition) =>writer.parquet(spark,path,partition,target,target.saveMode)
      case RedisConnector(host,port,auth,tbl,key) =>writer.redis(spark,host,port,auth,tbl,key)
      case _ => {
        val p = Promise[Result[String]]()
        Future {
          p failure ConnectorNotFoundException(s"target ${target.name} not found or have not been implemented yet.")
        }
        p.future
      }
    }

  }

}
