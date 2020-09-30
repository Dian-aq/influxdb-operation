package top.dianay.influxdb;

import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

public class InfluxDBHolder {

    private static Logger log = LoggerFactory.getLogger(InfluxDBHolder.class);

    static {
        InfluxdbProperties.InfluxdbConfig defaultInfluxdbConfig = InfluxdbProperties.getDefaultInfluxdbConfig();
        if (null != defaultInfluxdbConfig) {
            defaultInfluxDBConnect = init(InfluxdbProperties.getDefaultInfluxdbConfig());
        } else {
            log.warn("没有配置influxdb信息!");
        }
    }
    public static String dataBase;

    private static InfluxDB influxDBConnect;

    private static InfluxDB defaultInfluxDBConnect;

    private InfluxDBHolder() {
    }

    private static InfluxDB init(InfluxdbProperties.InfluxdbConfig influxDBConfig) {
        InfluxDB influxDB;
        log.info("======init InfluxDBConnectHolder======");
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        //写超时设置
        builder.writeTimeout(5, TimeUnit.SECONDS);
        builder.readTimeout(60, TimeUnit.SECONDS);
        //初始化连接池
        int connKeepLiveTime = 8;
        ConnectionPool connectionPool = new ConnectionPool(influxDBConfig.getConnectionNum(), connKeepLiveTime, InfluxdbProperties.timeUnit);
        builder.connectionPool(connectionPool);
        Dispatcher dispatcher = new Dispatcher();
        //最大并发请求数
        dispatcher.setMaxRequestsPerHost(influxDBConfig.getConnectionNum());
        builder.dispatcher(dispatcher);
        log.info("======influxDB connect to " + influxDBConfig.toString() + "======");
        influxDB = InfluxDBFactory.connect(influxDBConfig.getUrl(), influxDBConfig.getUsername(), influxDBConfig.getPassword(), builder);
        influxDB.setDatabase(influxDBConfig.getDataBase());
        dataBase = influxDBConfig.getDataBase();
        return influxDB;
    }

    public static void setConnect(String key) {
        if (InfluxdbProperties.getDatabaseKeys().contains(key)) {
            InfluxdbProperties.InfluxdbConfig influxdbConfig = InfluxdbProperties.getDatabase().stream().filter(data -> data.getKey().equals(key)).collect(toList()).get(0);
            log.info("Influxdb切换至{}数据源", key);
            influxDBConnect = init(influxdbConfig);
        } else {
            log.info("Influxdb切换失败!!  没有该数据源{}", key);
        }
    }

    /**
     * 清除上下文数据
     */
    public static void removeDataSourceRouterKey() {
        log.info("Influxdb重置至{}数据源", "默认");
        influxDBConnect = null;
        dataBase = InfluxdbProperties.getDefaultInfluxdbConfig().dataBase;

    }

    public static InfluxDB getConnect() {
        if (influxDBConnect == null && defaultInfluxDBConnect == null) {
            throw new RuntimeException(" not initial ");
        } else {
            return influxDBConnect == null ? defaultInfluxDBConnect : influxDBConnect;
        }
    }

    /**
     * 测试连接是否正常
     *
     * @return true 正常
     */
    public static boolean ping() {
        boolean isConnected = false;

        log.info("======check influxDB connect ======");

        if (influxDBConnect == null) {
            throw new RuntimeException(" not initial ");
        }
        Pong pong;
        try {
            pong = influxDBConnect.ping();
            if (pong != null) {
                isConnected = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isConnected;
    }
}
