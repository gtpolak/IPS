package com.example.demo.scenesControllers;

import com.example.demo.Type;
import com.example.demo.clickhouse.ClickHouseService;
import com.example.demo.firebird.FirebirdService;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.*;

@Component
public class MainSceneController {

    @FXML public Button button;
    @FXML public Label firebirdNotifications;
    @FXML public ComboBox<String> firebirdTables;
    @FXML public TextField firebirdSizeOfBatch;

    //private final FireBirdConnector fireBirdConnector;
    private final ClickHouseService clickHouseService;
    private final FirebirdService firebirdService;

    public MainSceneController(ClickHouseService clickHouseService, FirebirdService firebirdService) {
        this.clickHouseService = clickHouseService;
        //this.fireBirdConnector = fireBirdConnector;
        this.firebirdService = firebirdService;
    }

    @FXML
    public void copyToClickHouse() {
        String tableNameToCopy = firebirdTables.getSelectionModel().getSelectedItem();
        if (tableNameToCopy == null) {
            firebirdNotifications.setText("Nie wybrałeś tabeli");
            return;
        }

        if (clickHouseService.getAllTables().stream().anyMatch(s -> s.equals(tableNameToCopy))) {
            Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
            alert.setTitle("Tabela o danej nazwie już istnieje");
            alert.setHeaderText("Tabela o danej nazwie już istnieje");
            alert.setContentText("Usunąć ją?");

            Optional<ButtonType> result = alert.showAndWait();
            if (result.isPresent() && result.get() == ButtonType.OK) {
                try {
                    clickHouseService.deleteTable(tableNameToCopy);
                } catch (SQLException e) {
                    e.printStackTrace();
                    firebirdNotifications.setText("Błąd podczas usuwania tabeli");
                }
            } else {
                firebirdNotifications.setText("Anulowano");
                return;
            }
        }

        if (clickHouseService.createTable(tableNameToCopy, firebirdService.getColumnNameAndTypeFromTable(tableNameToCopy))) {
            int sizeOfBatch;
            try {
                sizeOfBatch = Integer.parseInt(firebirdSizeOfBatch.getText());
            } catch (NumberFormatException e){
                e.printStackTrace();
                firebirdNotifications.setText("Zły format rozmiaru paczek");
                return;
            }
        } else {
            firebirdNotifications.setText("Błąd podczas tworzenia tabeli w clickHouse");
        }

    }

    @FXML
    public void initialize() {
        //button.setOnAction( actionEvent -> System.out.println("Kliknięte"));

        Set<String> firebirdTabsNames = firebirdService.getTablesName();

        firebirdTables.getItems().addAll(firebirdTabsNames.toArray(new String[0]));

        //boolean result = firebirdService.createTable("NAZWA_TABELI", columnNameAndType);

        System.out.println("=================================================");
        System.out.println("===================ClickHouse====================");
        System.out.println("=================================================");

        //clickHouseService.getAllTables().forEach(System.out::println);
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

        //conn.close();
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
    }
}
