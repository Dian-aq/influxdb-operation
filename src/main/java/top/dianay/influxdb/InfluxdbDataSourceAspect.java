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

/**
 * @Author: dian
 * @Date: 2020/9/28 11:38
 * @Description: Influxdb数据源切换
 */
@Aspect
@Order(-1) // 保证该AOP在@Transactional之前执行
@Component
public class InfluxdbDataSourceAspect {
    private static final Logger logger = LoggerFactory.getLogger(InfluxdbDataSourceAspect.class);

    /**
     * 设置切面范围
     */
    @Pointcut("@within(top.dianay.influxdb.InfluxdbDataSource) || @annotation(top.dianay.influxdb.InfluxdbDataSource)")
    public void pointCut() {

    }

    @Before("pointCut() && @annotation(dataSource)")
    public void changeDataSource(JoinPoint point, InfluxdbDataSource dataSource) {
        String key = dataSource.name();
        InfluxDBHolder.setConnect(key);
    }

    @After("pointCut()")
    public void restoreDataSource() {
        InfluxDBHolder.removeDataSourceRouterKey();

    }
}
