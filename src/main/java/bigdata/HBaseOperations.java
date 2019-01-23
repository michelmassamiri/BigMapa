package bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class HBaseOperations extends Configured implements Tool {

    private static final byte[] POSFAMILY = Bytes.toBytes("positions");
    private static final byte[] DATAFAMILY = Bytes.toBytes("data");
    private static final byte[] TABLENAME = Bytes.toBytes("michelmassamiri");
    private static final byte[] LATMIN = Bytes.toBytes( "latmin");
    private static final byte[] LATMAX = Bytes.toBytes( "latmax");
    private static final byte[] LONGMIN = Bytes.toBytes(" longmin");
    private static final byte[] LONGMAX = Bytes.toBytes( "longmax");
    private static final byte[] ZOOM = Bytes.toBytes( "zoom");
    private static final byte[] IMAGE = Bytes.toBytes( "image");

    private static Connection connection;
    private static Table table;

    public static void create(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createTable(Connection connect) {
        try {
            final Admin admin = connect.getAdmin();
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLENAME));
            HColumnDescriptor famPosition = new HColumnDescriptor(POSFAMILY);
            HColumnDescriptor famData = new HColumnDescriptor(DATAFAMILY);

            descriptor.addFamily(famPosition);
            descriptor.addFamily(famData);
            create(admin, descriptor);
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static void createRow(String row, double latMin, double longMin, double latMax, double longMax, int zoom
            , BufferedImage bf) {

        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(POSFAMILY, ZOOM, Bytes.toBytes(zoom));
        put.addColumn(POSFAMILY, LATMIN, Bytes.toBytes(latMin));
        put.addColumn(POSFAMILY, LONGMIN, Bytes.toBytes(longMin));
        put.addColumn(POSFAMILY, LATMAX, Bytes.toBytes(latMax));
        put.addColumn(POSFAMILY, LONGMAX, Bytes.toBytes(longMax));

        /* Insert Image as png */
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ImageIO.write(bf, "png", baos);
            baos.flush();
            byte[] imageInByte = baos.toByteArray();
            put.addColumn(DATAFAMILY, IMAGE, imageInByte);
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    public int run(String[] args) throws IOException{
        connection = ConnectionFactory.createConnection(getConf());
        createTable(connection);

        table = connection.getTable(TableName.valueOf(TABLENAME));
        return 0;
    }
}

