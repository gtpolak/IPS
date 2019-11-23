package com.example.demo.scenesControllers;

import com.example.demo.clickhouse.ClickHouseConnection;
import com.example.demo.firebird.FirebirdService;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

@Component
public class MainSceneController {

    @FXML
    public Button button;

    @FXML
    public ComboBox<String> firebirdTables;

    //private final FireBirdConnector fireBirdConnector;

    private final FirebirdService firebirdService;
    public MainSceneController(FirebirdService firebirdService) {
        //this.fireBirdConnector = fireBirdConnector;
        this.firebirdService = firebirdService;
    }

    @FXML
    public void initialize(){
        button.setOnAction( actionEvent -> System.out.println("Kliknięte"));

        Set<String> firebirdTabsNames = firebirdService.getTablesName();

        firebirdTables.getItems().addAll(firebirdTabsNames.toArray(new String[1]));

        firebirdService.getColumnNameAndTypeFromTable("test");

//        Session session = hibernateUtil.getSessionFactory().getCurrentSession();
//        session.beginTransaction();
//        session.createSQLQuery("create table test2(" +
//                "id int not null primary key," +
//                "name varchar(20));").executeUpdate();
//        session.getTransaction().commit();
//        session.close();

//        List<Object[]> rows = new ArrayList<>();
//        rows.add(new Object[]{12, LocalDate.now()});
//
//        try {
//            ClickHouseClient clickHouseClient = new ClickHouseClient("http://localhost:8123", "default", "");
//
//            clickHouseClient.get("select * from test.test", Test.class).thenAccept(stringClickHouseResponse -> stringClickHouseResponse.data.forEach(test -> System.out.println(test.getId() + " " + test.getCos().toString())));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        try {
//
//            Connection conn = new ClickHouseConnection().getConnection();
//
//            Statement stmt = conn.createStatement();
//            String sql = "show databases";//
//            ResultSet rs = stmt.executeQuery(sql);
//            List<String> dbsList = new ArrayList<>();
//            while (rs.next()) {
//                dbsList.add(rs.getString(1));
//                System.out.println(rs.getString(1));
//            }
//            conn.commit();
//            rs.close();
//            stmt.close();
//            stmt = conn.createStatement();
//            //conn.close();
//            for(int i = 1; i < dbsList.size(); i++){
//                if(!dbsList.get(i).startsWith("default") && !dbsList.get(i).startsWith("system")) {
//                    //conn = dataSource.getConnection();
//                    stmt = conn.createStatement();
//                    System.out.println(dbsList.get(i));
//                    ResultSet resultSet = stmt.executeQuery("select count(*) from " + dbsList.get(i));
//                    resultSet.next();
//                    System.out.println(resultSet.getInt(1));
//                    ;
//                    conn.commit();
//                    rs.close();
//                    stmt.close();
//                    //conn.close();
//                }
//            }
//            Map<String, String> tableDesc = new LinkedHashMap<>();
//            for(int i = 1; i < dbsList.size(); i++){
//                if(!dbsList.get(i).startsWith("default") && !dbsList.get(i).startsWith("system")) {
//                    //conn = dataSource.getConnection();
//                    stmt = conn.createStatement();
//                    System.out.println(dbsList.get(i));
//                    ResultSet resultSet = stmt.executeQuery("desc " + dbsList.get(i));
//                    while(resultSet.next()){
//                        tableDesc.put(resultSet.getString(1), resultSet.getString(2));
//                    }
//                    conn.commit();
//                    rs.close();
//                    stmt.close();
//                }
//            }
//            for(Map.Entry<String, String> entry : tableDesc.entrySet()){
//                System.out.println(entry.getKey() + " " + entry.getValue());
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }

    }
}
