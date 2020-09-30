package top.dianay.influxdb;

import java.lang.annotation.*;


@Target({ElementType.METHOD, ElementType.TYPE, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface InfluxdbDataSource {
    String value() ;
}
