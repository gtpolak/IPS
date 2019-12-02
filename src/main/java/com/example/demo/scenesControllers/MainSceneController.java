package com.example.demo.scenesControllers;

import com.example.demo.clickhouse.ClickHouseService;
import com.example.demo.firebird.FirebirdService;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

@Component
public class MainSceneController {

    @FXML public Button button;
    @FXML public Label firebirdNotifications;
    @FXML public ComboBox<String> firebirdTables;
    @FXML public TextField firebirdSizeOfBatch;
    @FXML public TextArea logArea;
    @FXML public Button firebirdCSVButton;

    private final ClickHouseService clickHouseService;
    private final FirebirdService firebirdService;
    private Stage stage;

    public MainSceneController(ClickHouseService clickHouseService, FirebirdService firebirdService) {
        this.clickHouseService = clickHouseService;
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
        firebirdCSVButton.setOnAction(event -> {
            FileChooser.ExtensionFilter extensionFilter = new FileChooser.ExtensionFilter("CSV File (*.csv)", "*.csv");
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Wybierz plik .csv");
            fileChooser.getExtensionFilters().add(extensionFilter);
            try {
                firebirdService.importCsv(fileChooser.showOpenDialog(stage));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Set<String> firebirdTabsNames = firebirdService.getTablesName();

        firebirdTables.getItems().addAll(firebirdTabsNames.toArray(new String[0]));
    }

    public void setStage(Stage stage){
        this.stage = stage;
    }
}
