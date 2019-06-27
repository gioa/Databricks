// Databricks notebook source
// MAGIC %scala print(1)

// COMMAND ----------

// MAGIC %sql show tables 

// COMMAND ----------

sql("select * from diamonds").createOrReplaceTempView("diamonds_test1")

// COMMAND ----------

// MAGIC %sql select * from diamonds_test1

// COMMAND ----------

sc.textFile("dbfs:/FileStore/*.t*").count()

// COMMAND ----------

dbutils.fs.ls("FileStore")

// COMMAND ----------

// DBTITLE 0,Databases
// MAGIC %sql
// MAGIC CREATE WIDGET COMBOBOX database DEFAULT "default" CHOICES show databases;
// MAGIC show databases;

// COMMAND ----------

// DBTITLE 0,Tables
// use selected database
val dbname = dbutils.widgets.get("database")
sqlContext.sql(s"use ${dbname}")

// display list of tables in selected database
val tables = sqlContext.sql("show tables")
display(tables.toDF.select("tableName"))

// COMMAND ----------

// MAGIC %sql 

// COMMAND ----------

// DBTITLE 1,Create tables dropdown
// use selected database
val dbname = dbutils.widgets.get("database")
sqlContext.sql(s"use ${dbname}")

// remove table input if it exists
scala.util.control.Exception.ignoring(classOf[com.databricks.dbutils_v1.InputWidgetNotDefined]) {
  dbutils.widgets.remove("table")
}

val tableNames = tables.toDF.select("tableName").collect().map(r => r(0).toString)
  .take(1023) // dropdown widget limit

if (tableNames.length > 0) {
  dbutils.widgets.dropdown("table", tableNames(0), tableNames)
}

// COMMAND ----------

// DBTITLE 1,Table Schema
// use selected database
val dbname = dbutils.widgets.get("database")
sqlContext.sql(s"use ${dbname}")

val tableName = dbutils.widgets.get("table")
display(sqlContext.sql(s"describe ${tableName}"))

// COMMAND ----------

// DBTITLE 1,Preview Table
// use selected database
val dbname = dbutils.widgets.get("database")
sqlContext.sql(s"use ${dbname}")

val tableName = dbutils.widgets.get("table")
display(sqlContext.sql(s"select * from ${tableName} limit 20"))

// COMMAND ----------

// DBTITLE 1,Clean up input widgets
// dbutils.widgets.removeAll()