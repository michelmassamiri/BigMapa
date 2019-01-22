package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.File;
import java.nio.ByteBuffer;

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

        rddHeights.foreach(heights -> {
            // Bytes per pixel = 3;
            // Width/Height = 1201;
            ByteBuffer buffer = ByteBuffer.allocate(3 * 1201 * 1201);
            for (short h : heights) {
                switch(h) {
                    case 0 : buffer.put(new byte[] {(byte) 0, (byte) 128, (byte) 128}); break;
                    default : buffer.put(new byte[] {(byte) h, (byte) 0, (byte) 0}); break;
                }
            }
            buffer.rewind();
            DataBufferByte dbb = new DataBufferByte(buffer.array(), buffer.capacity());
            WritableRaster raster = Raster.createInterleavedRaster(
                    dbb,
                    1201, 1201,
                    1201 * 3,
                    3,
                    new int []{ 0, 1, 2 },
                    null);

            ColorModel colorModel =  new ComponentColorModel(
                    ColorSpace.getInstance(ColorSpace.CS_sRGB),
                    new int[] { 8, 8, 8 },
                    false,
                    false,
                    Transparency.OPAQUE,
                    DataBuffer.TYPE_BYTE);

            BufferedImage bfImage = new BufferedImage(colorModel, raster, false, null);
            bfImage = (bfImage, 256, 256);

            ImageIO.write(bfImage, "png", new File("output.png"));

        });

    }
}
