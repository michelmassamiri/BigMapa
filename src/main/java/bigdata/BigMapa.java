package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

public class BigMapa {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Test");
        JavaSparkContext context = new JavaSparkContext(conf);
        TileOperations tileOperations = new TileOperations();

        JavaPairRDD<String, PortableDataStream> rdd = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N44W001.hgt");
        JavaPairRDD<String, short[]> rddLatLong = tileOperations.extractHeightLatLong(rdd);
        tileOperations.generateTiles(rddLatLong);

        Tuple2<String, short[]> arcachon = rddLatLong.first();
        System.out.println(arcachon._1);
    }
}
