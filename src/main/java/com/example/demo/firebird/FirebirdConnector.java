package com.example.demo.firebird;

import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Component
public class FirebirdConnector {

    private Connection connection = null;

    private FirebirdConnector() {

    }

    public Connection getConnection() {
        if (connection == null) {
            try {
                Class.forName("org.firebirdsql.jdbc.FBDriver");
                connection = DriverManager.getConnection("jdbc:firebirdsql://127.0.0.1:3050/C:\\fbDB\\DATABASE.FDB", "SYSDBA", "password");
                connection.setAutoCommit(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public Statement createStatement() throws SQLException {
        Statement statement = null;
        statement = getConnection().createStatement();

        return statement;
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
}
