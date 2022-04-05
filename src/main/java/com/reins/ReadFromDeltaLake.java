package com.reins;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.delta.tables.DeltaTable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ReadFromDeltaLake {

        private static String warehouseDir = "hdfs://10.0.0.203:9000/delta/warehouse";
        private static String tableName = "taxi_version_1";

        public static void main(String[] args) {
                SparkSession spark = SparkSession.builder()
                                .config("spark.master", "local")
                                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                                .config("spark.sql.catalog.spark_catalog",
                                                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                .config("spark.sql.warehouse.dir", warehouseDir)
                                .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                                                "jdbc:mysql://rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/delta_metastore")
                                .config("spark.hadoop.javax.jdo.option.ConnectionDriverName",
                                                "com.mysql.cj.jdbc.Driver")
                                .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "zzt")
                                .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "Zzt19980924x")
                                .config("spark.hadoop.datanucleus.autoCreateSchema", true)
                                .config("spark.hadoop.datanucleus.autoCreateTables", true)
                                .config("spark.hadoop.datanucleus.fixedDatastore", false)
                                .config("spark.hadoop.datanucleus.readOnlyDatastore", false)
                                .config("spark.hadoop.datanucleus.autoStartMechanism", "SchemaTable")
                                .config("spark.hadoop.datanucleus.autoStartMechanism", "SchemaTable")
                                .config("spark.hadoop.hive.metastore.schema.verification", false)
                                .config("spark.hadoop.hive.metastore.schema.verification.record.version", false)
                                .enableHiveSupport()
                                .getOrCreate();
                spark.sparkContext().setLogLevel("WARN");

                Dataset<Row> df = spark.read().format("delta").table(tableName);
                df.show();
                Long count = spark.read().format("delta").table(tableName).count();
                log.warn("query count:{}", count);

                DeltaTable deltaTable = DeltaTable.forName(tableName);
                deltaTable.generate("symlink_format_manifest");

                spark.sql("use default");
                spark.sql("show tables").show();

                String sql = String.format("select * from %s", tableName);
                spark.sql(sql).show();
                log.warn("count:{}", spark.sql(sql).count());

                spark.catalog().listDatabases().show();
                spark.catalog().listTables().show();
                spark.close();
        }
}
