package com.example.demo.scenesControllers;

import com.example.demo.HibernateUtil;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import org.springframework.stereotype.Component;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

@Component
public class MainSceneController {

    @FXML
    public Button button;

    private final HibernateUtil hibernateUtil;
    public MainSceneController(HibernateUtil hibernateUtil) {
        this.hibernateUtil = hibernateUtil;
    }

    @FXML
    public void initialize(){
        button.setOnAction( actionEvent -> System.out.println("Kliknięte"));
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
        try {
            String sourceAppAddr = "127.0.0.1:8123";
            String jdbcConfig = "jdbc:clickhouse://" + sourceAppAddr;
            ClickHouseProperties properties = new ClickHouseProperties();
            properties.setMaxExecutionTime(100);
            properties.setUser("default");
            properties.setPassword("");
            ClickHouseDataSource dataSource = new ClickHouseDataSource(jdbcConfig + "", properties);
            Connection conn = dataSource.getConnection();

            Statement stmt = conn.createStatement();
            String sql = "select * from test";//
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                System.out.println(rs.getInt(1) + " " + rs.getDate(2));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
