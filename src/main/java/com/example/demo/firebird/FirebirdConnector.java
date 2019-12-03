package com.example.demo.firebird;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;


import org.springframework.stereotype.Component;

import java.sql.*;

@Component
public class FirebirdConnector {

    private Connection connection = null;

    private FirebirdConnector(){

    }

    private Connection getConnection() {
        if(connection == null){
            try{
                Class.forName("org.firebirdsql.jdbc.FBDriver");
                connection = DriverManager.getConnection("jdbc:firebirdsql://127.0.0.1:3050/C:\\fbDB\\DATABASE.FDB", "SYSDBA", "password");
                connection.setAutoCommit(true);
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        return connection;
    }

    public Statement createStatement(){
        Statement statement = null;
        try {
            statement = getConnection().createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return statement;
    }

    public void close(){
        try {
            connection.close();
            connection = null;
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
