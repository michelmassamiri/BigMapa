package bigdata;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.color.ColorSpace;
import java.awt.image.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TileOperations  {
    private static final int size = 1201;

    public static JavaPairRDD<String, short[]> extractHeightLatLong(JavaPairRDD<String, PortableDataStream> rddEntry) {
        JavaPairRDD<String, short[]> rddHeightsLatLong = rddEntry.mapToPair(stringPortableDataStreamTuple2 -> {
            /* Extract 1st and last latitude, longitude of the tile */
            String fileName = stringPortableDataStreamTuple2._1;
            String[] tokens = fileName.split("/");
            if(tokens[6] == null || StringUtils.isEmpty(tokens[6])) {
                throw new Exception("Error Path file for job spark");
            }

            int lat = Integer.parseInt(tokens[6].substring(1, 3));
            int lng = Integer.parseInt(tokens[6].substring(4, 7));
            if(tokens[6].contains("S") || tokens[6].contains("s")) lat = lat * -1;
            if(tokens[6].contains("W") || tokens[6].contains("w")) lng = lng * -1;
            //double latMax = lat + (double)1200 * 1./(double)size;
            //double lngMax = lng + (double)1200 * .001/(double)size;
            String newKey = String.valueOf(lat) + ',' + String.valueOf(lng);

            /* Extract the heights of the tile */
            PortableDataStream content = stringPortableDataStreamTuple2._2;
            byte[] pixels = content.toArray();
            int counter = 0;
            short[] height = new short[size * size];
            byte[] tmp = new byte[2];

            for(int i = 1;(counter < size * size) && (i < pixels.length); i+=2) {
                tmp[0] = pixels[i-1];
                tmp[1] = pixels[i];

                height[counter] = (short)(((tmp[0] & 0xFF) << 8) | (tmp[1]) & 0xFF);
                counter++;
            }
            return new Tuple2<>(newKey, height);
        });
        return rddHeightsLatLong;
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

    public static byte[] generatePng(short[] heights) throws IOException {
        // Bytes per pixel = 3;
        // Width/Height = 1201;
        ByteBuffer buffer = ByteBuffer.allocate(3 * 1201 * 1201);
        for (short height : heights) {
            buffer.put(short2color(height));
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
        bfImage = bfImage.getSubimage(0, 0, 1200, 1200);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(bfImage, "png", baos);
        baos.flush();
        byte[] imageInByte = baos.toByteArray();
        return imageInByte;
    }

    protected static Tile extractKey(String key, int zoom, short[] img) {
        String[] tokens = key.split(",");
        int latMin = Integer.parseInt(tokens[0]);
        int lngMin = Integer.parseInt(tokens[1]);
        int x = lngMin + 180;
        int y = latMin + 90;

        return new Tile(x, y, zoom, img);
    }

    protected static JavaPairRDD<String, Tile>  mapToForthZoom(JavaPairRDD<String, short[]> rddEntry, int zoom, int diviser) {
        JavaPairRDD<String, Tile> rddZoomFour = rddEntry.mapToPair(stringTuple2 -> {

            short[] heights = stringTuple2._2;
            Tile tile = TileOperations.extractKey(stringTuple2._1, zoom, heights);

            String newKey = tile.getX()/diviser + "," + tile.getY()/diviser + "," + tile.getZoom();
            return new Tuple2<>(newKey, tile);
        })
        .reduceByKey((v1, v2) -> {
            /* Calcul de la moyenne de deux valeurs */

            return null;
        });

        return rddZoomFour;
    }

}
