package com.twq.spark.atlas.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

class SparkExtension extends (SparkSessionExtensions => Unit) {
  override def apply(e: SparkSessionExtensions): Unit = {

  }
}

case class SparkAtlasListenerParser(spark: SparkSession, delegate: ParserInterface)
  extends ParserInterface {
  override def parsePlan(sqlText: String): LogicalPlan = {
    SQLQuery.set(sqlText)
    delegate.parsePlan(sqlText)
  }

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)
}

object SQLQuery {
  private[this] val sqlQuery = new ThreadLocal[String]
  def get(): String = sqlQuery.get
  def set(s: String): Unit = sqlQuery.set(s)
}
