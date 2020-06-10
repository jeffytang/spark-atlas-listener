/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twq.spark.atlas.sql

import com.twq.spark.atlas.WithHiveSupport
import com.twq.spark.atlas.sql.testhelper.BaseHarvesterSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

import scala.util.Random

abstract class BaseInsertIntoHarvesterSuite
  extends BaseHarvesterSuite {

  private val sourceHiveTblName = "source_h_" + Random.nextInt(100000)
  private val sourceSparkTblName = "source_s_" + Random.nextInt(100000)
  private val destinationHiveTblName = "destination_h_" + Random.nextInt(100000)
  private val destinationSparkTblName = "destination_s_" + Random.nextInt(100000)

  private val inputTable1 = "input1_" + Random.nextInt(100000)
  private val inputTable2 = "input2_" + Random.nextInt(100000)
  private val inputTable3 = "input3_" + Random.nextInt(100000)
  private val outputTable1 = "output1_" + Random.nextInt(100000)
  private val outputTable2 = "output2_" + Random.nextInt(100000)
  private val outputTable3 = "output3_" + Random.nextInt(100000)
  private val bigTable = "big_" + Random.nextInt(100000)

  protected override def initializeTestEnvironment(): Unit = {
    prepareDatabase()

    _spark.sql(s"CREATE TABLE $sourceHiveTblName (name string)")
    _spark.sql(s"INSERT INTO TABLE $sourceHiveTblName VALUES ('a'), ('b'), ('c')")

    _spark.sql(s"CREATE TABLE $sourceSparkTblName (name string) USING ORC")
    _spark.sql(s"INSERT INTO TABLE $sourceSparkTblName VALUES ('d'), ('e'), ('f')")

    _spark.sql(s"CREATE TABLE $destinationHiveTblName (name string)")
    _spark.sql(s"CREATE TABLE $destinationSparkTblName (name string) USING ORC")

    // tables used in cases of multiple source/destination tables
    _spark.sql(s"CREATE TABLE $inputTable1 (a int)")
    _spark.sql(s"INSERT INTO $inputTable1 VALUES(1)")
    _spark.sql(s"CREATE TABLE $inputTable2 (b int)")
    _spark.sql(s"INSERT INTO $inputTable2 VALUES(1)")
    _spark.sql(s"CREATE TABLE $inputTable3 (c int)")
    _spark.sql(s"INSERT INTO $inputTable3 VALUES(1)")
    _spark.sql(s"create table $outputTable1 (a int)")
    _spark.sql(s"create table $outputTable2 (b int)")
    _spark.sql(s"create table $outputTable3 (c int)")
    _spark.sql(s"CREATE TABLE $bigTable (a int, b int, c int)")
    _spark.sql(s"INSERT INTO $bigTable VALUES(100, '150', 200)")
  }

  override protected def cleanupTestEnvironment(): Unit = {
    cleanupDatabase()
  }

  test("INSERT INTO HIVE TABLE FROM HIVE TABLE") {
    val qe = _spark.sql(s"INSERT INTO TABLE $destinationHiveTblName " +
      s"SELECT * FROM $sourceHiveTblName").queryExecution
    val qd = QueryDetail(qe, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHiveTable])
    val cmd = node.cmd.asInstanceOf[InsertIntoHiveTable]

    val entities = CommandsHarvester.InsertIntoHiveTableHarvester.harvest(cmd, qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (1)
      assertTable(inputs.head, sourceHiveTblName)
    }, outputs => {
      outputs.size should be (1)
      assertTable(outputs.head, destinationHiveTblName)
    })
  }

  test("INSERT INTO HIVE TABLE FROM HIVE TABLE (Cyclic)") {
    val qe = _spark.sql(s"INSERT INTO TABLE $destinationHiveTblName " +
      s"SELECT * FROM $destinationHiveTblName").queryExecution
    val qd = QueryDetail(qe, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHiveTable])
    val cmd = node.cmd.asInstanceOf[InsertIntoHiveTable]

    val entities = CommandsHarvester.InsertIntoHiveTableHarvester.harvest(cmd, qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (1)
      assertTable(inputs.head, destinationHiveTblName)
    }, outputs => {
      assert(outputs.isEmpty)
    })
  }

  test("INSERT INTO HIVE TABLE FROM SPARK TABLE") {
    val qe = _spark.sql(s"INSERT INTO TABLE $destinationHiveTblName " +
      s"SELECT * FROM $sourceSparkTblName").queryExecution
    val qd = QueryDetail(qe, 0L)

    assert(qe.sparkPlan.isInstanceOf[DataWritingCommandExec])
    val node = qe.sparkPlan.asInstanceOf[DataWritingCommandExec]
    assert(node.cmd.isInstanceOf[InsertIntoHiveTable])
    val cmd = node.cmd.asInstanceOf[InsertIntoHiveTable]

    val entities = CommandsHarvester.InsertIntoHiveTableHarvester.harvest(cmd, qd)
    validateProcessEntity(entities.head, _ => {}, inputs => {
      inputs.size should be (1)
      assertTable(inputs.head, sourceSparkTblName)
    }, outputs => {
      outputs.size should be (1)
      assertTable(outputs.head, destinationHiveTblName)
    })
  }

}


class InsertIntoHarvesterSuite
  extends BaseInsertIntoHarvesterSuite
    with WithHiveSupport {

  override def beforeAll(): Unit = {
    super.beforeAll()
    initializeTestEnvironment()
  }

  override def afterAll(): Unit = {
    cleanupTestEnvironment()
    super.afterAll()
  }

  override protected def getSparkSession: SparkSession = sparkSession

  override protected def getDbName: String = "sac"

  override protected def expectSparkTableModels: Boolean = true
}