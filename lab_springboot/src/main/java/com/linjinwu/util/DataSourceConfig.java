package com.linjinwu.util;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Bean(name="postgresqlDataSource")
    @ConfigurationProperties(prefix="spring.datasource.druid.postgresql")
    public DataSource dataSource(){
        return new DruidDataSource();
    }

}
