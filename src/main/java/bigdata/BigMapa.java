package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

public class BigMapa  {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("Test");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<String, PortableDataStream> rdd = context.binaryFiles("hdfs://ripoux:9000/user/raw_data/dem3");

        Configuration hconf = HBaseConfiguration.create();
        hconf.set(TableInputFormat.INPUT_TABLE, "michelmassamiri");
        JobConf newAPIJobConfiguration = new JobConf(hconf, BigMapa.class);
        newAPIJobConfiguration.setOutputFormat(TableOutputFormat.class);
        newAPIJobConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "michelmassamiri");

        int exitCode = ToolRunner.run(hconf, new HBaseInit(), args);
        if(exitCode == 1) {
            System.exit(exitCode);
        }

        JavaPairRDD<String, short[]> rddLatLong = TileOperations.extractHeightLatLong(rdd);
        JavaPairRDD<ImmutableBytesWritable, Put> rddToSave = HbaseOperations.saveTileToHbase(rddLatLong);
        rddToSave.saveAsHadoopDataset(newAPIJobConfiguration);
    }
}
