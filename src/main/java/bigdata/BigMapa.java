package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

public class BigMapa {

    public static void main (String[] args) {

        SparkConf conf = new SparkConf().setAppName("Test");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<String, PortableDataStream> rdd = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N44W002.hgt");

        JavaRDD<short[]> rddHeights = rdd.map(stringPortableDataStreamTuple2 -> {
            //String fileName = stringPortableDataStreamTuple2._1;
            PortableDataStream content = stringPortableDataStreamTuple2._2;
            byte[] pixels = content.toArray();
            int size = 1201;
            int counter = 0;
            short[] height = new short[size * size];
            byte[] tmp = new byte[2];

            for(int i = 1; i < pixels.length; i+=2) {
                tmp[0] = pixels[i-1];
                tmp[1] = pixels[i];

                height[counter] = (short)(((tmp[0] & 0xFF) << 8) | (tmp[1]) & 0xFF);
                counter++;
            }
            return height;
        });

        rddHeights.foreach(ints -> {
            for(int i = 0; i < ints.length; ++i) {
                System.out.println(ints[i]);
            }
        });
    }
}
