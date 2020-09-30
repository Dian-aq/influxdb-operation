package top.dianay.influxdb;

import java.lang.annotation.*;

/**
 * @Author: dian
 * @Date: 2020/9/28 11:38
 * @Description: Influxdb数据源切换注解
 */
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface InfluxdbDataSource {
    String name() ;
}
