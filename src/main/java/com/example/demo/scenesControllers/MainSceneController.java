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
    @FXML public TextArea logArea;

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
        firebirdSizeOfBatch.setText("2");
        String tableName = firebirdTables.getSelectionModel().getSelectedItem();
        if (tableName == null) {
            firebirdNotifications.setText("Nie wybrałeś tabeli");
            return;
        }

        if (clickHouseService.getAllTables().stream().anyMatch(s -> s.equals(tableName))) {
            Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
            alert.setTitle("Tabela o danej nazwie już istnieje");
            alert.setHeaderText("Tabela o danej nazwie już istnieje");
            alert.setContentText("Usunąć ją?");

            Optional<ButtonType> result = alert.showAndWait();
            if (result.isPresent() && result.get() == ButtonType.OK) {
                try {
                    clickHouseService.deleteTable(tableName);
                } catch (SQLException e) {
                    e.printStackTrace();
                    firebirdNotifications.setText("Błąd podczas usuwania tabeli");
                }
            } else {
                firebirdNotifications.setText("Anulowano");
                return;
            }
        }
        int sizeOfBatch;
        if (clickHouseService.createTable(tableName, firebirdService.getColumnNameAndTypeFromTable(tableName))) {

            try {
                sizeOfBatch = Integer.parseInt(firebirdSizeOfBatch.getText());
            } catch (NumberFormatException e){
                //e.printStackTrace();
                firebirdNotifications.setText("Zły format rozmiaru paczek");
                return;
            }
        } else {
            firebirdNotifications.setText("Błąd podczas tworzenia tabeli w clickHouse");
            return;
        }
        int numberOfCols = firebirdService.getColumnNameAndTypeFromTable(tableName).size();
        int numberOfRows = firebirdService.getRowsCount(tableName);

        if(numberOfRows == 0){
            firebirdNotifications.setText("Tabela jest pusta");
            return;
        }
        int copiedRows = 1;
        while(copiedRows <= numberOfRows+1){
            try {
                List<List<String>> listOfData;
                if(copiedRows + sizeOfBatch > numberOfRows){
                    listOfData = firebirdService.getDataForBatchInsert(tableName, numberOfRows-copiedRows+1, copiedRows, numberOfCols);
                } else {
                    listOfData = firebirdService.getDataForBatchInsert(tableName, sizeOfBatch, copiedRows, numberOfCols);
                }
                listOfData.forEach(data -> {
                    try {
                        clickHouseService.insertBatchData(tableName, numberOfCols, data);
                    } catch (SQLException e) {
                        logArea.setText("Błąd podczas wstawiania danych");
                        return;
                    }
                });
            } catch (SQLException e){
                e.printStackTrace();
                firebirdNotifications.setText("Błąd podczas kopiowania danych");
                return;
            }
            copiedRows += sizeOfBatch;
        }
    }

    @FXML
    public void initialize() {
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
