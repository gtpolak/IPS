package com.example.demo;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;


import org.hibernate.service.ServiceRegistry;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class HibernateUtil {

    private static SessionFactory sessionFactory = null;

    private HibernateUtil(){
        this.sessionFactory = buildSessionFactory();
    }

    private synchronized static SessionFactory buildSessionFactory() {
        if(sessionFactory == null){
            try{
                Configuration configuration = new Configuration();

                Properties properties = new Properties();
                properties.setProperty(Environment.DRIVER, "org.firebirdsql.jdbc.FBDriver");
                properties.setProperty(Environment.URL, "jdbc:firebirdsql://127.0.0.1:3050/G:\\Firebird_3_0\\databases\\database.FDB");
                properties.setProperty(Environment.USER, "SYSDBA");
                properties.setProperty(Environment.PASS, "password");
                properties.setProperty(Environment.DIALECT, "org.hibernate.dialect.FirebirdDialect");
                properties.setProperty(Environment.CURRENT_SESSION_CONTEXT_CLASS, "org.hibernate.context.internal.ThreadLocalSessionContext");

                configuration.setProperties(properties);

                ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
                        .applySettings(configuration.getProperties()).build();

                sessionFactory = configuration.buildSessionFactory(serviceRegistry);
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        return sessionFactory;
    }


    public SessionFactory getSessionFactory() {
        return sessionFactory;
    }

}
