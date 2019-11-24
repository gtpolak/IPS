package com.example.demo.clickhouse;

import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Component
public class ClickHouseService {

    public List<String> getAllTables(){
        List<String> dbsList = new ArrayList<>();
        try {

            Connection conn = new ClickHouseConnection().getConnection();

            Statement stmt = conn.createStatement();
            String sql = "show tables";//
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                dbsList.add(rs.getString(1));
            }
            conn.commit();
            rs.close();
            stmt.close();
        } catch (Exception e){
            return null;
        }
        return dbsList;
    }
}
