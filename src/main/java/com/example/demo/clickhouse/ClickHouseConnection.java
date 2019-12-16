package com.example.demo.clickhouse;

import org.springframework.stereotype.Component;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Component
public class ClickHouseConnection {

    private Connection connection = null;

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
                connection = dataSource.getConnection();
                connection.setAutoCommit(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public Statement getStatement() throws SQLException {
        return getConnection().createStatement();
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}
