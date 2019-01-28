package bigdata;

import com.sun.jersey.core.util.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class Test {

    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("Test");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<String, PortableDataStream> rdd0 = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N43W001.hgt");
        JavaPairRDD<String, PortableDataStream> rdd1 = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N43W002.hgt");
        JavaPairRDD<String, PortableDataStream> rdd2 = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N44W001.hgt");
        JavaPairRDD<String, PortableDataStream> rdd3 = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N44W002.hgt");

        JavaPairRDD<String, short[]> rddShort0 = TileOperations.extractHeightLatLong(rdd0);
        JavaPairRDD<String, short[]> rddShort1 = TileOperations.extractHeightLatLong(rdd1);
        JavaPairRDD<String, short[]> rddShort2 = TileOperations.extractHeightLatLong(rdd2);
        JavaPairRDD<String, short[]> rddShort3 = TileOperations.extractHeightLatLong(rdd3);

        FileUtils.writeByteArrayToFile(new File("toto.png"), bytes);

        JavaPairRDD<String, short[]> rddShorts = context.union(rddShort0, rddShort1, rddShort2, rddShort3);

        // Tableau pour stocker le résultat
        short[] tmp = new short[1201 * 1201];
        System.out.println(tmp.length);

        rddShorts.reduceByKey((elem1, elem2)-> {
            
        }).mapToPair().;

            List<Tuple2<String, short[]>> list = Arrays.asList(new Tuple2<>("test", tmp));
        });

        List<Tuple2<String, short[]>> list = Arrays.asList(new Tuple2<>("test", tmp));

        JavaPairRDD<String, short[]> rddStringShort = context.parallelizePairs(list);
    }

}
