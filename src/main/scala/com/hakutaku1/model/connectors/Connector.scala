package com.hakutaku1.model.connectors

/**
  * Data connectors
  * @since 1.0
  * @auth Aaron Zhao
  */
trait Connector

abstract class FileConnector extends Connector


/**
  * Parquet file connector.
  * @param path: the absolute parquet file path (excluding partitions).
  * @param partitionCol: partition columns (keep order if multiple presents) separated with comma. WIll be used when writing as a partition.
  */
case class ParquetFileConnector(path:String,partitionCol:Option[String]=None) extends FileConnector

/**
  * Redis connector
  * @param host
  * @param port
  * @param auth
  * @param tbl
  * @param key
  */
case class RedisConnector(host:String,port:String,auth:String,tbl:String,key:String) extends Connector

