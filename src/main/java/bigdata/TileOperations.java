package bigdata;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.input.PortableDataStream;
import scala.Tuple2;

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
}
