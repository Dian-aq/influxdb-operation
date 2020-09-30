package top.dianay.influxdb;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Aspect
@Order(-1)
@Component
public class InfluxdbDataSourceAspect {

    /**
     * 设置切面范围
     */
    @Pointcut("@within(top.dianay.influxdb.InfluxdbDataSource) || @annotation(top.dianay.influxdb.InfluxdbDataSource)")
    public void pointCut() {

    }

    @Before("pointCut() && @annotation(dataSource)")
    public void changeDataSource(JoinPoint point, InfluxdbDataSource dataSource) {
        String key = dataSource.value();
        InfluxDBHolder.setConnect(key);
    }

    @After("pointCut()")
    public void restoreDataSource() {
        InfluxDBHolder.removeDataSourceRouterKey();
    }
}
