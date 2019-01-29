package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

public class BigMapa  {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("Test");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<String, PortableDataStream> rdd = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3");
        int numPartisions = context.sc().getExecutorIds().length();
        rdd.repartition(numPartisions);

        Configuration hconf = HBaseConfiguration.create();
        hconf.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, "michelmassamiri");
        Job newAPIJobConfiguration = Job.getInstance(hconf);
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
        //newAPIJobConfiguration.set(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.OUTPUT_TABLE, "michelmassamiri");
        FileOutputFormat.setOutputPath(newAPIJobConfiguration, new Path("hdfs://young:9000/user/mmassamire/tmp"));

        int exitCode = ToolRunner.run(hconf, new HBaseInit(), args);
        if(exitCode == 1) {
            System.exit(exitCode);
        }

        JavaPairRDD<String, short[]> rddLatLong = TileOperations.extractHeightLatLong(rdd).cache();
        JavaPairRDD<String, Tile> rddZoom5 = TileOperations.mapToForthZoom(rddLatLong, 5, 2).cache();

        //JavaPairRDD<ImmutableBytesWritable, Put> rddToSaveZoom6 = HbaseOperations.saveTileToHbase(rddLatLong).cache();
        //rddToSaveZoom6.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());

        JavaPairRDD<ImmutableBytesWritable, Put> rddToSaveZoom5 = HbaseOperations.saveToHbase(rddZoom5).cache();
        rddToSaveZoom5.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());


        context.close();
    }
}
