package bigdata;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

public class BigMapa  {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("Test");
        JavaSparkContext context = new JavaSparkContext(conf);
        HBaseOperations hBaseOperations = new HBaseOperations();

        JavaPairRDD<String, PortableDataStream> rdd = context.binaryFiles("hdfs://ripoux:9000/user/raw_data/dem3/N44W001.hgt");
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), hBaseOperations, args);
        if(exitCode == 1) {
            System.exit(exitCode);
        }

        TileOperations tileOperations = new TileOperations();
        JavaPairRDD<String, short[]> rddLatLong = tileOperations.extractHeightLatLong(rdd);
        tileOperations.generateTiles(rddLatLong);

        Tuple2<String, short[]> arcachon = rddLatLong.first();
        System.out.println(arcachon._1);
    }
}
