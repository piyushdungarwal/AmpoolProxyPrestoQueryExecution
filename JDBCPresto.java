import java.sql.*;
import java.util.Properties;

public class JDBCPresto {

    public static void main(String args[]){
        Connection connection = null;
        Statement statement = null;
        try {
            Class.forName("io.prestosql.jdbc.PrestoDriver");

            Properties properties = new Properties();
            properties.setProperty("user", "demokey");
            properties.setProperty("password", "4YUkMPEoJZC15bUd");
            properties.setProperty("SSL", "true");
            //properties.setProperty("SSLTrustStorePath", "PATH/ampool_truststore");
            //properties.setProperty("SSLTrustStorePassword", "ampool");
            properties.setProperty("AllowSelfSignedServerCert", "true");
            connection = DriverManager.getConnection("jdbc:presto://172.31.5.86:9295/ampoolproxy/default", properties);
            statement = connection.createStatement();

            String sql = "SELECT s_name, count(*) AS numwait FROM ampoolproxy.tpch002.supplier_csv, ampoolproxy.tpch002.lineitem_gp L1, ampoolproxy.tpch002.orders_gp, ampoolproxy.tpch002.nation_parquet WHERE s_suppkey = L1.l_suppkey AND o_orderkey = L1.l_orderkey   AND o_orderstatus = 'F'   AND L1.l_receiptdate > L1.l_commitdate  AND exists( SELECT * FROM  ampoolproxy.tpch002.lineitem_gp L2 WHERE L2.l_orderkey = L1.l_orderkey AND L2.l_suppkey <> L1.l_suppkey ) AND NOT exists( SELECT * FROM  ampoolproxy.tpch002.lineitem_gp L3   WHERE L3.l_orderkey = L1.l_orderkey AND L3.l_suppkey <> L1.l_suppkey  AND L3.l_receiptdate > L3.l_commitdate )   AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA' GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100";

            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData rsmd = resultSet.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            for (int i = 1; i <= columnsNumber; i++) {
                System.out.print(rsmd.getColumnName(i) + " ");
            }

            System.out.println();
            System.out.println("====================================================");
            while(resultSet.next()){
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = resultSet.getString(i);
                    System.out.print(columnValue + " ");
                }
                System.out.println("");
            }
            resultSet.close();
            statement.close();
            connection.close();
        }catch(SQLException sqlException){
            sqlException.printStackTrace();
        }catch(Exception exception){
            exception.printStackTrace();
        }
    }
}
