package com.example.demo.clickhouse;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseConnection {

    Connection connection = null;

    public Connection getConnection() {
        if (connection == null) {
            try {
                String sourceAppAddr = "127.0.0.1:8123";
                String jdbcConfig = "jdbc:clickhouse://" + sourceAppAddr;
                ClickHouseProperties properties = new ClickHouseProperties();
                properties.setMaxExecutionTime(100);
                properties.setUser("default");
                properties.setPassword("");
                ClickHouseDataSource dataSource = new ClickHouseDataSource(jdbcConfig + "", properties);
                return dataSource.getConnection();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
}
