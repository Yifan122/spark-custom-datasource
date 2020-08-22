# Spark custom DataSource

-- This project is mainly modified from http://shzhangji.com/blog/2018/12/08/spark-datasource-api-v2/

Spark support several format: csv, json, parquet, orc, etc.

ex: 

```scala
val peopleDFCsv = spark.read.format("csv")
  .option("sep", ";")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("examples/src/main/resources/people.csv")
```



## DataSource V1 API

Some basic API for DataSource V1:

```scala
trait RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
}

abstract class BaseRelation {
  def sqlContext: SQLContext
  def schema: StructType
}

trait TableScan {
  def buildScan(): RDD[Row]
}
```

A `RelationProvider` defines a class that can create a relational data source for Spark SQL to manipulate with. It can initialize itself with provided options, such as file path or authentication. `BaseRelation` is used to define the data schema, which can be loaded from database, Parquet file, or specified by the user. This class also needs to mix-in one of the `Scan` traits, implements the `buildScan` method, and returns an RDD.



![image-20200822221506101](C:\Users\Yifan\IdeaProjects\spark-custom-datasource\imgs\image-20200822221506101.png)

Spark will first to look into the relative path in the format, and check the **DefaultSource** class.



## DataSource V2 API

```scala
public interface DataSourceV2 {}

public interface ReadSupport extends DataSourceV2 {
  DataSourceReader createReader(DataSourceOptions options);
}

public interface DataSourceReader {
  StructType readSchema();
  List<DataReaderFactory<Row>> createDataReaderFactories();
}

public interface SupportsPushDownRequiredColumns extends DataSourceReader {
  void pruneColumns(StructType requiredSchema);
}

public interface DataReaderFactory<T> {
  DataReader<T> createDataReader();
}

public interface DataReader<T> extends Closeable {
  boolean next();
  T get();
}
```

