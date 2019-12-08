package com.example.demo.scenesControllers;

import com.example.demo.clickhouse.ClickHouseService;
import com.example.demo.firebird.FirebirdService;
import com.example.demo.models.Type;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;

@Component
public class MainSceneController {

    @FXML public Button button;
    @FXML public Label firebirdNotifications;
    @FXML public ComboBox<String> firebirdTables;
    @FXML public TextField firebirdSizeOfBatch;
    @FXML public TextArea logArea;
    @FXML public Button firebirdCSVButton;
    @FXML public Button firebirdDeleteTable;
    @FXML public Button clickHouseDeleteTable;
    @FXML public Button clickHouseCSVButton;
    @FXML public ComboBox<String> clickHouseTables;
    @FXML public TextField clickHouseSizeOfBatch;

    private final ClickHouseService clickHouseService;
    private final FirebirdService firebirdService;
    private Stage stage;

    public MainSceneController(ClickHouseService clickHouseService, FirebirdService firebirdService) {
        this.clickHouseService = clickHouseService;
        this.firebirdService = firebirdService;
    }

    @FXML
    public void copyToFirebird(){
        String tableName = clickHouseTables.getSelectionModel().getSelectedItem();
        if(tableName == null){
            clearAndAppendToLogArea("Wybierz tabele");
            return;
        }
        Map<String, Type> tableDesc;
        try {
            tableDesc = clickHouseService.getTableDescription(tableName);
        } catch (SQLException e) {
            clearAndAppendToLogArea("Błąd podczas pobierania typów danych tabeli");
            e.printStackTrace();
            return;
        }

        if(firebirdService.getAllTables().stream().anyMatch(s -> s.equals(tableName))) {
            Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
            alert.setTitle("Wybierz opcje");
            alert.setHeaderText("Usunąć tabele " + tableName + "?");
            alert.setContentText("Przed rozpoczęciem kopiowania należy usunąć tabelę");
            Optional<ButtonType> result = alert.showAndWait();
            if(!result.isPresent()){
                clearAndAppendToLogArea("Nie można kontynuować, ponieważ anulowano usunięcie tabeli");
                return;
            }
            if(result.get() == ButtonType.OK){
                try {
                    firebirdService.dropTable(tableName);
                } catch (SQLException e) {
                    e.printStackTrace();
                    clearAndAppendToLogArea("Błąd podczas usuwania tabeli");
                }
            } else {
                clearAndAppendToLogArea("Anulowano usunięcie tabeli");
                return;
            }
        }

        Map<String, Type> firebirdType = firebirdService.convertToFirebirdDataTypes(tableDesc);

        if(firebirdService.createTable(tableName, firebirdType)){
            refreshFirebirdTables();
            appendToLogArea("Utworzono tabelę " + tableName + " w bazie Firebird");
        } else {
            appendToLogArea("Błąd podczas tworzenia tabeli " + tableName);
            return;
        }

        int tableCount = 0;
        try {
            tableCount = clickHouseService.getTableCount(tableName);
        } catch (SQLException e) {
            clearAndAppendToLogArea("Błąd podczas pobierania liczby danych do skopiowania");
            e.printStackTrace();
            return;
        }

        int batchSize = 0;
        try{
            batchSize = Integer.parseInt(clickHouseSizeOfBatch.getText());
        } catch (NumberFormatException e){
            clearAndAppendToLogArea("Zły format rozmiaru paczki");
            return;
        }
        appendToLogArea("Rozpoczęto kopiowanie tabeli " + tableName + " z bazy CLickHouse do bazy Firebird");
        int copyCount = 0;
        while(copyCount < tableCount){
            List<String[]> data;
            try {
                data = clickHouseService.getBatchData(tableName, copyCount, batchSize, firebirdType.size());
            } catch (SQLException e) {
                e.printStackTrace();
                clearAndAppendToLogArea("Błąd podczas pobierania danych");
                return;
            }
            try {
                firebirdService.insertBatchData(data, firebirdType, tableName);
            } catch (SQLException e) {
                e.printStackTrace();
                clearAndAppendToLogArea("Błąd podczas wstawiania danych");
                return;
            }
            copyCount += batchSize;
        }
        appendToLogArea("Zakończono kopiowanie tabeli " + tableName + " z bazy CLickHouse do bazy Firebird");
        firebirdService.closeConnection();
    }

    @FXML
    public void copyToClickHouse() {
        logArea.clear();
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
        Map<String, Type> colTypes = firebirdService.getColumnNameAndTypeFromTable(tableName);
        if (clickHouseService.createTable(tableName, colTypes)) {
            logArea.appendText(LocalDateTime.now().toString() + " - utworzono tabele " + tableName + " w bazie ClickHouse");
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
        String[] colsTypesArray = new String[colTypes.size()];
        int i = 0;
        for(Map.Entry<String, Type> entry : colTypes.entrySet()){
            colsTypesArray[i] = entry.getValue().getTypeName();
            i++;
        }
        logArea.appendText(LocalDateTime.now().toString() + " - rozpoczęto kopiowanie tabeli " + tableName + " do bazy ClickHouse");
        int copiedRows = 1;
        while(copiedRows < numberOfRows){
            try {
                List<String[]> listOfData;
                listOfData = firebirdService.getDataForBatchInsert(tableName, sizeOfBatch, copiedRows, numberOfCols);
                clickHouseService.insertBatchData(tableName, colTypes, listOfData);
            } catch (SQLException e){
                e.printStackTrace();
                firebirdNotifications.setText("Błąd podczas kopiowania danych");
                return;
            }
            copiedRows += sizeOfBatch;
        }
        logArea.appendText(LocalDateTime.now().toString() + " - zakończono kopiowanie tabeli " + tableName + " do bazy ClickHouse");
        clickHouseService.closeConnection();
    }

    @FXML
    public void initialize() {
        firebirdDeleteTable.setOnAction(event -> {
            try {
                firebirdService.dropTable(firebirdTables.getSelectionModel().getSelectedItem());
                refreshFirebirdTables();
            } catch (SQLException e) {
                e.printStackTrace();
                firebirdNotifications.setText(e.getMessage());
            }
        });

        firebirdCSVButton.setOnAction(event -> {
            FileChooser.ExtensionFilter extensionFilter = new FileChooser.ExtensionFilter("CSV File (*.csv)", "*.csv");
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Wybierz plik .csv");
            fileChooser.getExtensionFilters().add(extensionFilter);
            try {
                firebirdService.importCsv(fileChooser.showOpenDialog(stage), logArea);
                refreshFirebirdTables();
            } catch (IOException | SQLException | NullPointerException e) {
                if(e instanceof NullPointerException){
                    logArea.clear();
                    logArea.appendText("Plik nie został wybrany");
                }
            }
        });
        Set<String> firebirdTabsNames = firebirdService.getAllTables();

        firebirdTables.getItems().addAll(firebirdTabsNames);

        Set<String> clickHouseTablesNames = clickHouseService.getAllTables();
        clickHouseTables.getItems().addAll(clickHouseTablesNames);
    }

    @FXML
    public void dropClickHouseTable(){
        String tableName = clickHouseTables.getSelectionModel().getSelectedItem();
        try {
            clickHouseService.deleteTable(tableName);
        } catch (SQLException e) {
            logArea.clear();
            logArea.appendText(LocalDateTime.now().toString() + " - błąd podczas usuwania tabeli " + tableName + " z ClickHouse\n");
            e.printStackTrace();
        }
        logArea.appendText(LocalDateTime.now().toString() + " - usunięto tabelę " + tableName + " z ClickHouse\n");
        refreshClickHouseTables();
    }

    private void refreshClickHouseTables() {
        Set<String> clickHouseTablesNames = clickHouseService.getAllTables();
        clickHouseTables.getItems().removeAll(clickHouseTables.getItems());
        clickHouseTables.getItems().addAll(clickHouseTablesNames);
    }

    private void refreshFirebirdTables() {
        Set<String> firebirdTabsNames = firebirdService.getAllTables();
        firebirdTables.getItems().removeAll(firebirdTables.getItems());
        firebirdTables.getItems().addAll(firebirdTabsNames.toArray(new String[0]));
    }

    public void setStage(Stage stage){
        this.stage = stage;
    }

    public void clearAndAppendToLogArea(String message){
        logArea.clear();
        logArea.appendText(LocalDateTime.now().toString() + " - " + message);
    }

    public void appendToLogArea(String message){
        logArea.appendText(LocalDateTime.now().toString() + " - " + message);
    }
}
