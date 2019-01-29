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

        JavaPairRDD<String, PortableDataStream> rdd0 = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N44W002.hgt");
        JavaPairRDD<String, PortableDataStream> rdd1 = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N44W001.hgt");
        JavaPairRDD<String, PortableDataStream> rdd2 = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N43W002.hgt");
        JavaPairRDD<String, PortableDataStream> rdd3 = context.binaryFiles("hdfs://young:9000/user/raw_data/dem3/N43W001.hgt");

        JavaPairRDD<String, short[]> rddShort0 = TileOperations.extractHeightLatLong(rdd0);
        JavaPairRDD<String, short[]> rddShort1 = TileOperations.extractHeightLatLong(rdd1);
        JavaPairRDD<String, short[]> rddShort2 = TileOperations.extractHeightLatLong(rdd2);
        JavaPairRDD<String, short[]> rddShort3 = TileOperations.extractHeightLatLong(rdd3);

        short[] rddShorts0 = rddShort0.first()._2;
        short[] rddShorts1 = rddShort1.first()._2;
        short[] rddShorts2 = rddShort2.first()._2;
        short[] rddShorts3 = rddShort3.first()._2;

        byte[] bytes0 = TileOperations.generatePng(rddShorts0);
        byte[] bytes1 = TileOperations.generatePng(rddShorts1);
        byte[] bytes2 = TileOperations.generatePng(rddShorts2);
        byte[] bytes3 = TileOperations.generatePng(rddShorts3);

        //
        BufferedImage tmp01 = new BufferedImage(1200, 1200, BufferedImage.TYPE_INT_RGB);
        BufferedImage img0 = ImageIO.read(new ByteArrayInputStream(bytes0));
        BufferedImage img1 = ImageIO.read(new ByteArrayInputStream(bytes1));
        Graphics g = tmp01.getGraphics();
        g.drawImage(resize(img0, 600,600), 0, 0, null);
        g.drawImage(resize(img1, 600,600), 600, 0, null);
        // IMG 0 + 1
        // return tmp01 to Bytes

        // 2 + 3
        BufferedImage tmp23 = new BufferedImage(1200, 1200, BufferedImage.TYPE_INT_RGB);
        BufferedImage img2 = ImageIO.read(new ByteArrayInputStream(bytes2));
        BufferedImage img3 = ImageIO.read(new ByteArrayInputStream(bytes3));
        Graphics g2 = tmp23.getGraphics();
        g2.drawImage(resize(img2, 600,600), 0, 600, null);
        g2.drawImage(resize(img3, 600,600), 600, 600, null);

        // [2+3] + [0+1]
        BufferedImage last = new BufferedImage(1200, 1200, BufferedImage.TYPE_INT_RGB);
        for (int y = 0; y < 1200; ++y) {
            for (int x = 0; x < 1200; ++x) {
                last.setRGB(x, y, (tmp01.getRGB(x, y) + tmp23.getRGB(x, y)));
            }
        }

        ImageIO.write(tmp01, "png", new File("tmp01.png"));
        ImageIO.write(tmp23, "png", new File("tmp23.png"));
        ImageIO.write(last, "png", new File("toto.png"));

        // fonction
        private static Tile zoom(Tile t1, Tile t2) {
            int div = getDiv(t1);
            int x = getX(t1);
            int y = getY(t1);
            if (!t1.isIntermediate) {
                BufferedImage tmp1 = new BufferedImage(1200, 1200, BufferedImage.TYPE_INT_RGB);
                Graphics g1 = tmp1.getGraphics();
                g.drawImage(resize(t1.getImage, 1200 / div, 1200 / div), x * 1200 / div, y * 1200 / div);
            }

            // pareil pour t2

            if(!t2.isIntermediate) {
                BufferedImage tmp2 = new BufferedImage(1200, 1200, BufferedImage.TYPE_INT_RGB);
                Graphics g2 = tmp2.getGraphics();
                g2.drawImage(resize(t2.getImage, 1200 / div, 1200 / div), x * 1200 / div, y * 1200 / div);
            }

            BufferedImage res = new BufferedImage(1200, 1200, BufferedImage.TYPE_INT_RGB);
            for (int j = 0; y < 1200; ++y) {
                for (int i = 0; x < 1200; ++x) {
                    last.setRGB(i, j, (tmp01.getRGB(i, j) + tmp23.getRGB(i, j)));
                }
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(res, "png", baos);
            baos.flush();

            return new Tile(key.getX, key.getY, zoom, baos.toByteArray());
        }



    }

    private static BufferedImage resize(BufferedImage img, int newW, int newH) {
        Image tmp = img.getScaledInstance(newW, newH, Image.SCALE_SMOOTH);
        BufferedImage dimg = new BufferedImage(newW, newH, BufferedImage.TYPE_INT_RGB);

        Graphics2D g2d = dimg.createGraphics();
        g2d.drawImage(tmp, 0, 0, null);
        g2d.dispose();

        return dimg;
    }
}


