package com.example.demo.scenesControllers;

import com.example.demo.clickhouse.ClickHouseService;
import com.example.demo.firebird.FirebirdService;
import com.example.demo.models.Type;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

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
    private boolean isCopying = false;

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
        System.out.println("Table count = " + tableCount);
        int batchSize = 0;
        try{
            batchSize = Integer.parseInt(clickHouseSizeOfBatch.getText());
        } catch (NumberFormatException e){
            clearAndAppendToLogArea("Zły format rozmiaru paczki");
            return;
        }
        int finalBatchSize = batchSize;
        int finalTableCount = tableCount;
        Task<Void> task = new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                int copyCount = 0;
                while(copyCount < finalTableCount){
                    List<String[]> data;
                    try {
                        data = clickHouseService.getBatchData(tableName, copyCount, finalBatchSize, firebirdType.size());
                    } catch (SQLException e) {
                        e.printStackTrace();
                        clearAndAppendToLogArea("Błąd podczas pobierania danych");
                        return null;
                    }
                    try {
                        firebirdService.insertBatchData(data, firebirdType, tableName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        clearAndAppendToLogArea("Błąd podczas wstawiania danych");
                        return null;
                    }
                    copyCount += finalBatchSize;
                }
                appendToLogArea("Zakończono kopiowanie tabeli " + tableName + " z bazy CLickHouse do bazy Firebird");
                firebirdService.closeConnection();
                return null;
            }
        };
        AtomicReference<Alert> alert = new AtomicReference<>();
        //alert.set(new Alert(Alert.AlertType.INFORMATION));
        task.setOnRunning(event -> {
            alert.set(new Alert(Alert.AlertType.INFORMATION));
            alert.get().setTitle("Trwa kopiowanie, proszę czekać");
            alert.get().setContentText("Trwa kopiowanie, proszę czekać. Po zakończeniu zostanie podany stosowny komunikat do pola z logami");
            alert.get().showAndWait();
        });
        task.setOnSucceeded(event -> {
            alert.get().close();
            appendToLogArea("Zakończono kopiowanie tabeli " + tableName + " z bazy CLickHouse do bazy Firebird");
            restartFirebirdService();
        });
        appendToLogArea("Rozpoczęto kopiowanie tabeli " + tableName + " z bazy CLickHouse do bazy Firebird");
        new Thread(task).start();
    }

    private void restartFirebirdService() {
            new Thread(() -> {
                try {
                    Runtime.getRuntime().exec("net stop FireBirdServerDefaultInstance");
                    Thread.sleep(1000);
                    Runtime.getRuntime().exec("net start FireBirdServerDefaultInstance");
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
    }

    @FXML
    public void copyToClickHouse() {
        logArea.clear();
        String tableName = firebirdTables.getSelectionModel().getSelectedItem();
        if (tableName == null) {
            clearAndAppendToLogArea("Nie wybrałeś tabeli");
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
                    clearAndAppendToLogArea("Błąd podczas usuwania tabeli");
                }
            } else {
                clearAndAppendToLogArea("Anulowano");
                return;
            }
        }
        int sizeOfBatch;
        Map<String, Type> colTypes = firebirdService.getColumnNameAndTypeFromTable(tableName);
        if (clickHouseService.createTable(tableName, colTypes)) {
            appendToLogArea(LocalDateTime.now().toString() + " - utworzono tabele " + tableName + " w bazie ClickHouse");
            try {
                sizeOfBatch = Integer.parseInt(firebirdSizeOfBatch.getText());
            } catch (NumberFormatException e){
                //e.printStackTrace();
                clearAndAppendToLogArea("Zły format rozmiaru paczek");
                return;
            }
        } else {
            clearAndAppendToLogArea("Błąd podczas tworzenia tabeli w clickHouse");
            return;
        }
        int numberOfCols = firebirdService.getColumnNameAndTypeFromTable(tableName).size();
        int numberOfRows = firebirdService.getRowsCount(tableName);

        if(numberOfRows == 0){
            clearAndAppendToLogArea("Tabela " + tableName + " jest pusta");
            return;
        }
        String[] colsTypesArray = new String[colTypes.size()];
        int i = 0;
        for(Map.Entry<String, Type> entry : colTypes.entrySet()){
            colsTypesArray[i] = entry.getValue().getTypeName();
            i++;
        }
        Task<Void> task = new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                int copiedRows = 1;
                while(copiedRows < numberOfRows){
                    try {
                        List<String[]> listOfData;
                        listOfData = firebirdService.getDataForBatchInsert(tableName, sizeOfBatch, copiedRows, numberOfCols);
                        clickHouseService.insertBatchData(tableName, colTypes, listOfData);
                    } catch (SQLException e){
                        e.printStackTrace();
                        appendToLogArea("Błąd podczas kopiowania danych do ClickHouse");
                        return null;
                    }
                    copiedRows += sizeOfBatch;
                }
                return null;
            }
        };
        AtomicReference<Alert> alert = new AtomicReference<>();
        //alert.set(new Alert(Alert.AlertType.INFORMATION));
        task.setOnRunning(event -> {
            alert.set(new Alert(Alert.AlertType.INFORMATION));
            alert.get().setTitle("Trwa kopiowanie, proszę czekać");
            alert.get().setContentText("Trwa kopiowanie, proszę czekać. Po zakończeniu zostanie podany stosowny komunikat do pola z logami");
            alert.get().showAndWait();
        });
        task.setOnSucceeded(event -> {
            alert.get().close();
            appendToLogArea("Zakończono kopiowanie tabeli " + tableName + " z bazy Firebird do bazy ClickHouse");
            restartFirebirdService();
        });
        appendToLogArea("Rozpoczęto kopiowanie tabeli " + tableName + " z bazy Firebird do bazy ClickHouse");
        new Thread(task).start();
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

        clickHouseCSVButton.setOnAction(event -> {
            FileChooser.ExtensionFilter extensionFilter = new FileChooser.ExtensionFilter("CSV File (*.csv)", "*.csv");
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Wybierz plik .csv");
            fileChooser.getExtensionFilters().add(extensionFilter);
            try {
                clickHouseService.importCsv(fileChooser.showOpenDialog(stage), logArea);
                refreshFirebirdTables();
            } catch (IOException | SQLException | NullPointerException e) {
                if(e instanceof NullPointerException){
                    appendToLogArea("Plik nie został wybrany");
                }
                if(e instanceof SQLException){
                    ((SQLException) e).printStackTrace();
                    appendToLogArea("Błąd podczas wstawiania danych do bazy ClickHouse");
                }
                if(e instanceof IOException){
                    ((IOException) e).printStackTrace();
                    appendToLogArea("Błąd podczas czytania pliku");
                }
            }
        });
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
        logArea.appendText(LocalDateTime.now().toString() + " - " + message + "\n");
    }

    public void appendToLogArea(String message){
        logArea.appendText(LocalDateTime.now().toString() + " - " + message + "\n");
    }
}
