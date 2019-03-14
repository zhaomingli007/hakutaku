package com.hakutaku1.model.jobs

package workflow.model

import com.hakutaku1.model.connectors.Connector
import org.apache.spark.sql.SaveMode


/**
  * Data target.
  * @param name: the target name
  * @param desc: the description of this data source
  * @param owner: the data owner of this data source
  * @param coalesce: the number of files will be coalesced, only apply for file targets.
  * @param connector: the data target's connector.
  * @param saveMode: overwrite,append etc. in SaveMode .
  */
case class Target(name:String ,desc:String ,owner:String ,coalesce:Int=1,connector:Connector
                  ,saveMode:String="Overwrite",isAddTimeStamp:Boolean=false)


/**
  * Data source, readable with Spark.
  * @param id: source id
  * @param name: the source name
  * @param desc: the description of this data source
  * @param owner: the data owner of this data source
  * @param mapTable: the table name to be registered in spark SQL temporarily.
  * @param connector: the data source's connector.
  * @param loadStgy: loading strategy.
  */
case class Source[+C](id:Int,name:String,desc:String,owner:String,mapTable:String,connector:C)

/**
  * Data connctors
  * @since 1.0
  * @auth Aaron Zhao
  */
abstract class Job(name:String, ver:String, id:Int)


/**
  * Job represent a complete running entity that will be scheduled individually.
  * @param name Job name.
  * @param ver Job version.
  * @param id Job id.
  * @param sqlFile absolute file path for the customized spark SQL.
  * @param sources data sources to be transformed from.
  * @param target target where the result will be store to.
  * @param timeout kill job in timeout minutes.
  */
case class SparkJob(name:String, ver:String, id:Int,failRollback:Boolean=true
                    , failRetry:Boolean=true, retryInterval:Int=5, retryTimes:Int=2,
                    sqlFile:String, sources:List[Source[Connector]], target:Target,
                     appId:Int, appName:String, timeout:Int=30) extends Job(name,ver,id)

