package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import javax.xml.bind.DatatypeConverter;

public class BigMapa {

    public static void main (String[] args) {

        SparkConf conf = new SparkConf().setAppName("Test");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<String, PortableDataStream> rdd = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/s87w180.hgt");
        /*
        JavaRDD<Object> rddObject = rdd.map(stringPortableDataStreamTuple2 -> {
            String fileName = stringPortableDataStreamTuple2._1;
            PortableDataStream content = stringPortableDataStreamTuple2._2;

            byte[] pixels = content.toArray();
            String base64Encoded = DatatypeConverter.printBase64Binary(pixels);
            return (fileName + "," + base64Encoded).getBytes();
        });
        */

        JavaRDD<byte[]> rddBytes = rdd.map(stringPortableDataStreamTuple2 -> {
            //String fileName = stringPortableDataStreamTuple2._1;
            PortableDataStream content = stringPortableDataStreamTuple2._2;

            byte[] pixels = content.toArray();
            //byte[] base64Encoded = DatatypeConverter.printBase64Binary(pixels).getBytes();

            // TODO
            // hgt stocke en short big indian (2 octets inversés)

            return pixels;
        });


        //System.out.println(rddObject.collect().get(0));

        //JavaRDD<byte[]> test = context.binaryRecords("hdfs://young:9000/user/raw_data/dem3/s87w180.hgt", 16);
        //System.out.println(test.count());

    }
}
