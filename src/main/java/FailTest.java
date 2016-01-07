import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.FailoverDBClusterRequest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

public class FailTest {

    private static String initialAuroraUrl;
    private static String accessKey;
    private static String secretKey;
    private static String clusterIdentifier;
    private static final AtomicInteger counter = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        //default aurora url (like jdbc:mariadb:aurora://instance-1,instance-2,instance-3/test?user=mariadb&password=xxx&connectTimeout=3500)
        initialAuroraUrl = System.getProperty("defaultAuroraUrl");
        accessKey = System.getProperty("AWS-ACCESS-KEY"); //Aurora access key
        secretKey = System.getProperty("AWS-SECRET-KEY"); //Aurora secret key
        clusterIdentifier = System.getProperty("AWS-CLUSTER-ID"); //Cluster ID

        if (initialAuroraUrl == null || accessKey == null || secretKey == null || clusterIdentifier == null) {
            throw new IllegalArgumentException("missing parameters");
        }

        HikariConfig config = new HikariConfig();
        config.addDataSourceProperty("url", initialAuroraUrl);
        config.setDataSourceClassName("org.mariadb.jdbc.MariaDbDataSource");
        config.setMinimumIdle(50);
        config.setMaximumPoolSize(150);
        config.setConnectionTimeout(80000);
        config.setIdleTimeout(60000);

        HikariDataSource datasource = new HikariDataSource(config);
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
        AmazonRDSClient amazonRdsClient = new AmazonRDSClient(awsCreds);
        FailoverDBClusterRequest failoverDbClusterRequest = new FailoverDBClusterRequest();
        failoverDbClusterRequest.setDBClusterIdentifier(clusterIdentifier);

        for (int i = 0; i < 600; i++) {
            Thread tt = new Thread(new RetrieveConnection(i, datasource));
            tt.setName("test-thread-" + i);
            tt.start();

            //let's get some time for the pool to grow
            if (i < 200) {
                Thread.sleep(600 - 3 * i);
            }

            Thread.sleep(250);

            //When pool is really launched, call the amazon API for a failover.
            if (i == 300) {
                amazonRdsClient.failoverDBCluster(failoverDbClusterRequest);
                System.err.println("Failover !");
            }
        }

        //wait for all thread to finish
        while (counter.get() != 0) {
            Thread.sleep(350);
        }

        datasource.close();
        System.exit(0);

    }

    /**
     * A thread that will do 130 "SELECT 1".
     * One third use the master connection, 2/3 a slave connection.
     */
    protected static class RetrieveConnection implements Runnable {
        DataSource datasource;
        int threadLaunchNumber;

        public RetrieveConnection(int threadLaunchNumber, DataSource datasource) {
            this.datasource = datasource;
            this.threadLaunchNumber = threadLaunchNumber;
        }

        public void run() {
            System.out.println("RUN");
            counter.incrementAndGet();
            Connection connection = null;
            try {
                connection = datasource.getConnection();
                if (threadLaunchNumber % 3 != 0) {
                    connection.setReadOnly(true);
                }
                for (int i = 0; i < 130; i++) {
                    try (Statement st = connection.createStatement()) {
                        st.execute("SELECT 1");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (connection != null) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            counter.decrementAndGet();
        }
    }

}
