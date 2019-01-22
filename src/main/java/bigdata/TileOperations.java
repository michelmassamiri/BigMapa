package bigdata;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.File;
import java.nio.ByteBuffer;

public class TileOperations {
    private static final int size = 1201;

    public JavaPairRDD<String, short[]> extractHeightLatLong(JavaPairRDD<String, PortableDataStream> rddEntry) {
        JavaPairRDD<String, short[]> rddHeightsLatLong = rddEntry.mapToPair(stringPortableDataStreamTuple2 -> {
            /* Extract 1st and last latitude, longitude of the tile */
            String fileName = stringPortableDataStreamTuple2._1;
            String[] tokens = fileName.split("/");
            if(tokens[6] == null || StringUtils.isEmpty(tokens[6])) {
                throw new Exception("Error Path file for job spark");
            }

            double lat = Double.parseDouble(tokens[6].substring(1, 3));
            double lng = Double.parseDouble(tokens[6].substring(4, 7));
            if(tokens[6].substring(0, 3).contains("S") || tokens[6].substring(1, 3).contains("s")) lat = lat * -1;
            if(tokens[6].substring(3, 7).contains("W") || tokens[6].substring(3, 7).contains("w")) lng = lng * -1;
            double latMin = lat;
            double lngMin = lng;
            double latMax = lat + (double)1200 * 1./(double)size;
            double lngMax = lng + (double)1200 * .001/(double)size;
            String newKey = '(' + String.valueOf(latMin)+ ',' + String.valueOf(lngMin)+ ')' + ',' +
                    '(' + String.valueOf(latMax )+ ',' + String.valueOf(lngMax )+ ')';
            /* Extract the heights of the tile */
            PortableDataStream content = stringPortableDataStreamTuple2._2;
            byte[] pixels = content.toArray();
            int counter = 0;
            short[] height = new short[size * size];
            byte[] tmp = new byte[2];

            for(int i = 1; i < pixels.length; i+=2) {
                tmp[0] = pixels[i-1];
                tmp[1] = pixels[i];

                height[counter] = (short)(((tmp[0] & 0xFF) << 8) | (tmp[1]) & 0xFF);
                counter++;
            }
            return new Tuple2<>(newKey, height);
        });
        return rddHeightsLatLong;
    }

    public void generateTiles(JavaPairRDD<String, short[]> rdd) {
        rdd.foreach(stringTuple2 -> {
            short[] heights = stringTuple2._2;
            // Bytes per pixel = 3;
            // Width/Height = 1201;
            ByteBuffer buffer = ByteBuffer.allocate(3 * 1201 * 1201);
            for (short h : heights) {
                switch (h) {
                    case 0:
                        buffer.put(new byte[]{(byte) 0, (byte) 128, (byte) 128});
                        break;
                    default:
                        buffer.put(new byte[]{(byte) h, (byte) 0, (byte) 0});
                        break;
                }
            }
            buffer.rewind();
            DataBufferByte dbb = new DataBufferByte(buffer.array(), buffer.capacity());
            WritableRaster raster = Raster.createInterleavedRaster(
                    dbb,
                    1201, 1201,
                    1201 * 3,
                    3,
                    new int[]{0, 1, 2},
                    null);

            ColorModel colorModel = new ComponentColorModel(
                    ColorSpace.getInstance(ColorSpace.CS_sRGB),
                    new int[]{8, 8, 8},
                    false,
                    false,
                    Transparency.OPAQUE,
                    DataBuffer.TYPE_BYTE);

            BufferedImage bfImage = new BufferedImage(colorModel, raster, false, null);

            ImageIO.write(bfImage, "png", new File("output.png"));

        });
    }
}
