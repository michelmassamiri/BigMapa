package bigdata;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.io.Serializable;

public class HbaseOperations implements Serializable {

    public static JavaPairRDD<ImmutableBytesWritable, Put> saveTileToHbase(JavaPairRDD<String, short[]> rdd){

        JavaPairRDD<ImmutableBytesWritable, Put> rddHeightsBytesPng = rdd.mapToPair(stringTuple2 -> {

            short[] heights = stringTuple2._2;
            byte[] image = TileOperations.generatePng(heights);
            Tile tile = TileOperations.extractKey(stringTuple2._1, 7);

            Put put = HBaseInit.createRow(tile, image);
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });

        return rddHeightsBytesPng;
    }

}
