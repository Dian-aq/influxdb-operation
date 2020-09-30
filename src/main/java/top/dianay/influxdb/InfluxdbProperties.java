package top.dianay.influxdb;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Configuration
@ConfigurationProperties(prefix = "spring.influx")
public class InfluxdbProperties {

    public static TimeUnit timeUnit = TimeUnit.SECONDS;

    /**
     * Influxdb数据库连接对象信息 集合
     */
    private static List<InfluxdbConfig> database = new LinkedList<InfluxdbConfig>();

    public static List<InfluxdbConfig> getDatabase() {
        return database;
    }

    public void setDatabase(List<InfluxdbConfig> databasep) {
        database = databasep;
    }

    public static List<String> getDatabaseKeys() {
        return database.stream().map(InfluxdbConfig::getKey).collect(Collectors.toList());
    }

    public static InfluxdbConfig getDefaultInfluxdbConfig() {
        if (CollectionUtils.isNotEmpty(database)) {
            return database.get(0);
        } else {
            return null;
        }
    }

    public static class InfluxdbConfig {

        String key;
        String url;
        String username;
        String password;
        String dataBase;
        Integer connectionNum = 5;

        public Integer getConnectionNum() {
            return connectionNum;
        }

        public void setConnectionNum(Integer connectionNum) {
            this.connectionNum = connectionNum;
        }

        public String getDataBase() {
            return dataBase;
        }

        public void setDataBase(String dataBase) {
            this.dataBase = dataBase;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}

