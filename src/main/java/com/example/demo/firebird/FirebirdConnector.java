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
//                Configuration configuration = new Configuration();
//
//
//
//                Properties properties = new Properties();
//                properties.setProperty(Environment.DRIVER, "org.firebirdsql.jdbc.FBDriver");
//                properties.setProperty(Environment.URL, "jdbc:firebirdsql://127.0.0.1:3050/G:\\Firebird_3_0\\databases\\database.FDB");
//                properties.setProperty(Environment.USER, "SYSDBA");
//                properties.setProperty(Environment.PASS, "password");
//                properties.setProperty(Environment.DIALECT, "org.hibernate.dialect.FirebirdDialect");
//                properties.setProperty(Environment.CURRENT_SESSION_CONTEXT_CLASS, "org.hibernate.context.internal.ThreadLocalSessionContext");
//
//                configuration.setProperties(properties);
//
//                ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
//                        .applySettings(configuration.getProperties()).build();

                Class.forName("org.firebirdsql.jdbc.FBDriver");
                connection = DriverManager.getConnection("jdbc:firebirdsql://127.0.0.1:3050/G:\\Firebird_3_0\\databases\\database.FDB", "SYSDBA", "password");
                connection.setAutoCommit(true);


                //sessionFactory = configuration.buildSessionFactory(serviceRegistry);
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
