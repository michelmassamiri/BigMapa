package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class HBaseInit extends Configured implements Tool {

    public static final byte[] TABLENAME = Bytes.toBytes("michelmassamiri");
    private static final byte[] POSFAMILY = Bytes.toBytes("positions");
    private static final byte[] DATAFAMILY = Bytes.toBytes("data");
    private static final byte[] X = Bytes.toBytes( "x");
    private static final byte[] Y = Bytes.toBytes( "y");
    private static final byte[] ZOOM = Bytes.toBytes( "zoom");
    private static final byte[] IMAGE = Bytes.toBytes( "image");

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

    public static Put createRow(Tile tile, byte[] bf) {
        String row = tile.getX() + "," + tile.getY();

        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(POSFAMILY, ZOOM, Bytes.toBytes(tile.getZoom()));
        put.addColumn(POSFAMILY, X, Bytes.toBytes(tile.getX()));
        put.addColumn(POSFAMILY, Y, Bytes.toBytes(tile.getY()));

        /* Insert Image as png */
        put.addColumn(DATAFAMILY, IMAGE, bf);
        return put;
    }

    @Override
    public int run(String[] args) throws IOException{
        Configuration conf = getConf();
        Connection connection = ConnectionFactory.createConnection(conf);
        createTable(connection);

        //table = connection.getTable(TableName.valueOf(TABLENAME));
        return 0;
    }
}

