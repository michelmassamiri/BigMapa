package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class BigMapa {

    public static void main (String[] args) {

        SparkConf conf = new SparkConf().setAppName("Test");
        JavaSparkContext context = new JavaSparkContext(conf);

        System.out.println("Hello");


    }
}
