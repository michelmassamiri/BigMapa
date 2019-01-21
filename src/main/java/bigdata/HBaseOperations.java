package bigdata;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class HBaseOperations {

    public static class HBaseProg extends Configured implements Tool {

        private static final byte[] TEST = Bytes.toBytes("test");
        private static final byte[] TILE = Bytes.toBytes("tile");
        private static final byte[] POS = Bytes.toBytes("pos");
        private static final byte[] LATMIN = Bytes.toBytes( "latmin");
        private static final byte[] LATMAX = Bytes.toBytes( "latmax");
        private static final byte[] LONGMIN = Bytes.toBytes(" longmin");
        private static final byte[] LONGMAX = Bytes.toBytes( "longmax");
        private static final byte[] ZOOM = Bytes.toBytes("zoom");
        private static final byte[] DATA = Bytes.toBytes("data");

        public static void create(Admin admin, HTableDescriptor table) throws IOException {
            if (admin.tableExists(table.getTableName())) {
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);
        }

        public static void createTable(Connection connect) throws IOException {
            final Admin admin = connect.getAdmin();
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TEST));
            HColumnDescriptor famPic = new HColumnDescriptor(TILE);
            // HColumnDescriptor



        }

                                  @Override
        public int run(String[] strings) throws Exception {
            return 0;
        }
    }


}
