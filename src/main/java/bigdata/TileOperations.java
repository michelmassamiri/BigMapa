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
                    buffer.put(short2color(h));
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

    private static byte[] short2color(short s) {
        if (s > 250) return new byte[] {(byte) 255, (byte) 245, (byte) 235};
        if (s > 225) return new byte[] {(byte) 253, (byte) 174, (byte) 107};
        if (s > 200) return new byte[] {(byte) 166, (byte) 54, (byte) 3};
        if (s > 175) return new byte[] {(byte) 127, (byte) 39, (byte) 4};
        if (s > 150) return new byte[] {(byte) 0, (byte) 68, (byte) 27};
        if (s > 50) return new byte[] {(byte) 35, (byte) 139, (byte) 69};
        if (s > 25) return new byte[] {(byte) 65, (byte) 171, (byte) 93};
        if (s > 10) return new byte[] {(byte) 116, (byte) 196, (byte) 118};
        if (s > 5) return new byte[] {(byte) 217, (byte)214, (byte)163};
        return new byte[] {(byte) 8, (byte) 48, (byte) 107};
    }

}
