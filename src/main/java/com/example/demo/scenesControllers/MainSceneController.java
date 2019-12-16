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

    @FXML
    public Button button;
    @FXML
    public Label firebirdNotifications;
    @FXML
    public ComboBox<String> firebirdTables;
    @FXML
    public TextField firebirdSizeOfBatch;
    @FXML
    public TextArea logArea;
    @FXML
    public Button firebirdCSVButton;
    @FXML
    public Button firebirdDeleteTable;
    @FXML
    public Button clickHouseDeleteTable;
    @FXML
    public Button clickHouseCSVButton;
    @FXML
    public ComboBox<String> clickHouseTables;
    @FXML
    public TextField clickHouseSizeOfBatch;

    private final ClickHouseService clickHouseService;
    private final FirebirdService firebirdService;
    private Stage stage;
    private boolean isCopying = false;

    public MainSceneController(ClickHouseService clickHouseService, FirebirdService firebirdService) {
        this.clickHouseService = clickHouseService;
        this.firebirdService = firebirdService;
    }

    @FXML
    public void copyToFirebird() {
        String tableName = clickHouseTables.getSelectionModel().getSelectedItem();
        if (tableName == null) {
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

        try {
            if (firebirdService.getAllTables().stream().anyMatch(s -> s.equals(tableName))) {
                Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
                alert.setTitle("Wybierz opcje");
                alert.setHeaderText("Usunąć tabele " + tableName + "?");
                alert.setContentText("Przed rozpoczęciem kopiowania należy usunąć tabelę");
                Optional<ButtonType> result = alert.showAndWait();
                if (!result.isPresent()) {
                    clearAndAppendToLogArea("Nie można kontynuować, ponieważ anulowano usunięcie tabeli");
                    return;
                }
                if (result.get() == ButtonType.OK) {
                    try {
                        appendToLogArea("Trwa usuwanie tabeli " + tableName + ". Aplikacja może w tym czasie nie odpowiadać. Proszę czekać");
                        firebirdService.dropTable(tableName);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        clearAndAppendToLogArea("Błąd podczas usuwania tabeli " + tableName);
                        return;
                    }
                } else {
                    clearAndAppendToLogArea("Anulowano usunięcie tabeli " + tableName);
                    return;
                }
                appendToLogArea("Pomyślnie usunięto tabele " + tableName + " z bazy FireBird");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            clearAndAppendToLogArea("Błąd podczas pobierania nazw tabel z Firebird");
        }

        Map<String, Type> firebirdType = firebirdService.convertToFirebirdDataTypes(tableDesc);

        if (firebirdService.createTable(tableName, firebirdType)) {
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
        try {
            batchSize = Integer.parseInt(clickHouseSizeOfBatch.getText());
        } catch (NumberFormatException e) {
            clearAndAppendToLogArea("Zły format rozmiaru paczki");
            return;
        }
        int finalBatchSize = batchSize;
        int finalTableCount = tableCount;
        Task<Void> task = new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                int copyCount = 0;
                while (copyCount < finalTableCount) {
                    List<String[]> data = null;
                    try {
                        data = clickHouseService.getBatchData(tableName, copyCount, finalBatchSize, firebirdType.size());
                        firebirdService.insertBatchData(data, firebirdType, tableName);
                        firebirdService.closeConnection();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        clearAndAppendToLogArea("Błąd podczas wstawiania danych");
                        return null;
                    }
                    copyCount += finalBatchSize;
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
            appendToLogArea("Zakończono kopiowanie tabeli " + tableName + " z bazy ClickHouse do bazy Firebird");
        });
        appendToLogArea("Rozpoczęto kopiowanie tabeli " + tableName + " z bazy ClickHouse do bazy Firebird (" + finalTableCount + " rekordów)");
        new Thread(task).start();
    }

    private void restartFirebirdService() {
        new Thread(() -> {
            try {
                appendToLogArea("Trwa restartowanie serwisu Firebird. Proszę czekać");
                Runtime.getRuntime().exec("net stop FireBirdServerDefaultInstance");
                Thread.sleep(3000);
                Runtime.getRuntime().exec("net start FireBirdServerDefaultInstance");
                appendToLogArea("Zakończono restartowanie serwisu Firebird");
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    @FXML
    public void copyToClickHouse() {
        String tableName = firebirdTables.getSelectionModel().getSelectedItem();
        if (tableName == null) {
            clearAndAppendToLogArea("Nie wybrałeś tabeli");
            return;
        }

        try {
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
        } catch (SQLException e) {
            e.printStackTrace();
            clearAndAppendToLogArea("Błąd podczas pobierania nazw tabel z ClickHouse");
            return;
        }
        int sizeOfBatch;
        Map<String, Type> colTypes = null;
        try {
            colTypes = firebirdService.getColumnNameAndTypeFromTable(tableName);
        } catch (SQLException e) {
            e.printStackTrace();
            clearAndAppendToLogArea("Błąd podczas pobierania typów danych dla tabeli " + tableName + " z bazy Firebird");
        }
        if (clickHouseService.createTable(tableName, colTypes)) {
            appendToLogArea(LocalDateTime.now().toString() + " - utworzono tabele " + tableName + " w bazie ClickHouse");
            try {
                sizeOfBatch = Integer.parseInt(firebirdSizeOfBatch.getText());
            } catch (NumberFormatException e) {
                clearAndAppendToLogArea("Zły format rozmiaru paczek");
                return;
            }
        } else {
            clearAndAppendToLogArea("Błąd podczas tworzenia tabeli w clickHouse");
            return;
        }
        refreshClickHouseTables();
        int numberOfCols = colTypes.size();
        int numberOfRows = 0;
        try {
            numberOfRows = firebirdService.getRowsCount(tableName);
        } catch (SQLException e) {
            e.printStackTrace();
            clearAndAppendToLogArea("Błąd podczas pobierania liczby rzędów z tabeli " + tableName + " z bazy Firebird");
            return;
        }

        if (numberOfRows == 0) {
            clearAndAppendToLogArea("Tabela " + tableName + " jest pusta");
            return;
        }
        int finalNumberOfRows = numberOfRows;
        Map<String, Type> finalColTypes = colTypes;
        colTypes = null;
        Task<Void> task = new Task<Void>() {
            @Override
            protected Void call() throws Exception {
                int copiedRows = 1;
                while (copiedRows < finalNumberOfRows) {
                    try {
                        List<String[]> listOfData;
                        listOfData = firebirdService.getDataForBatchInsert(tableName, sizeOfBatch, copiedRows, numberOfCols);
                        clickHouseService.insertBatchData(tableName, finalColTypes, listOfData);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        appendToLogArea("Błąd podczas kopiowania danych do ClickHouse");
                        return null;
                    }
                    copiedRows += sizeOfBatch;
                }
                clickHouseService.closeConnection();
                appendToLogArea("Zakończono kopiowanie tabeli " + tableName + " z bazy Firebird do bazy ClickHouse");
                return null;
            }
        };
        AtomicReference<Alert> alert = new AtomicReference<>();
        int finalNumberOfRows1 = numberOfRows;
        task.setOnRunning(event -> {
            appendToLogArea("Rozpoczęto kopiowanie tabeli " + tableName + " z bazy Firebird do bazy ClickHouse (" + finalNumberOfRows1 + " rekordów)");
            alert.set(new Alert(Alert.AlertType.INFORMATION));
            alert.get().setTitle("Trwa kopiowanie, proszę czekać");
            alert.get().setContentText("Trwa kopiowanie, proszę czekać. Po zakończeniu zostanie podany stosowny komunikat do pola z logami");
            alert.get().showAndWait();
        });
        task.setOnSucceeded(event -> {
            alert.get().close();
        });
        new Thread(task).start();
    }

    @FXML
    public void initialize() {
        firebirdDeleteTable.setOnAction(event -> {
            try {
                firebirdService.dropTable(firebirdTables.getSelectionModel().getSelectedItem());
                appendToLogArea("Usunięto tabele " + firebirdTables.getSelectionModel().getSelectedItem() + " z bazy Firebird");
                refreshFirebirdTables();
            } catch (SQLException e) {
                e.printStackTrace();
                appendToLogArea("Błąd podczas usuwania tabeli");
            }
        });

        firebirdCSVButton.setOnAction(event -> {
            FileChooser.ExtensionFilter extensionFilter = new FileChooser.ExtensionFilter("CSV File (*.csv)", "*.csv");
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Wybierz plik .csv");
            fileChooser.getExtensionFilters().add(extensionFilter);
            try {
                firebirdService.importCsv(fileChooser.showOpenDialog(stage), this);
            } catch (IOException | NullPointerException | NumberFormatException e) {
                if (e instanceof NullPointerException) {
                    clearAndAppendToLogArea("Plik nie został wybrany");
                }
                if (e instanceof IOException) {
                    ((IOException) e).printStackTrace();
                    clearAndAppendToLogArea("Błąd podczas odczytu pliku");
                }
                if (e instanceof NumberFormatException) {
                    clearAndAppendToLogArea("Zły format typu danych");
                }
            }
        });
        Set<String> firebirdTabsNames = null;
        try {
            firebirdTabsNames = firebirdService.getAllTables();
        } catch (SQLException e) {
            e.printStackTrace();
            appendToLogArea("Błąd podczas pobierania nazw tabel z Firebirda");
        }

        firebirdTables.getItems().addAll(firebirdTabsNames);

        Set<String> clickHouseTablesNames = null;
        try {
            clickHouseTablesNames = clickHouseService.getAllTables();
            clickHouseTables.getItems().addAll(clickHouseTablesNames);
        } catch (SQLException e) {
            e.printStackTrace();
            appendToLogArea("Błąd podczas pobierania nazw tabel z ClickHouse");
        }

        clickHouseCSVButton.setOnAction(event -> {
            FileChooser.ExtensionFilter extensionFilter = new FileChooser.ExtensionFilter("CSV File (*.csv)", "*.csv");
            FileChooser fileChooser = new FileChooser();
            fileChooser.setTitle("Wybierz plik .csv");
            fileChooser.getExtensionFilters().add(extensionFilter);
            try {
                clickHouseService.importCsv(fileChooser.showOpenDialog(stage), this);
                refreshFirebirdTables();
            } catch (IOException | NullPointerException e) {
                if (e instanceof NullPointerException) {
                    appendToLogArea("Plik nie został wybrany");
                }
                if (e instanceof IOException) {
                    ((IOException) e).printStackTrace();
                    appendToLogArea("Błąd podczas czytania pliku");
                }
            }
            refreshClickHouseTables();
        });
        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stage.setOnCloseRequest(event -> {
                try {
                    firebirdService.closeConnection();
                    clickHouseService.closeConnection();
                } catch (SQLException e) {
                    // do nothing
                }
            });
        }).start();
    }

    @FXML
    public void dropClickHouseTable() {
        String tableName = clickHouseTables.getSelectionModel().getSelectedItem();
        try {
            clickHouseService.deleteTable(tableName);
        } catch (SQLException e) {
            clearAndAppendToLogArea(LocalDateTime.now().toString() + " - błąd podczas usuwania tabeli " + tableName + " z ClickHouse");
            e.printStackTrace();
        }
        appendToLogArea(LocalDateTime.now().toString() + " - usunięto tabelę " + tableName + " z ClickHouse");
        refreshClickHouseTables();
    }

    public void refreshClickHouseTables() {
        Set<String> clickHouseTablesNames = null;
        try {
            clickHouseTablesNames = clickHouseService.getAllTables();
        } catch (SQLException e) {
            appendToLogArea("Błąd podczas pobierania nazw tabel z ClickHouse");
            return;
        }
        clickHouseTables.getItems().removeAll(clickHouseTables.getItems());
        clickHouseTables.getItems().addAll(clickHouseTablesNames);
        clickHouseTables.setPromptText("Wybierz tabele");
    }

    public void refreshFirebirdTables() {
        Set<String> firebirdTabsNames = null;
        try {
            firebirdTabsNames = firebirdService.getAllTables();
        } catch (SQLException e) {
            e.printStackTrace();
            appendToLogArea("Błąd podczas odświeżania tabel Firebird");
            return;
        }
        firebirdTables.getItems().removeAll(firebirdTables.getItems());
        firebirdTables.getItems().addAll(firebirdTabsNames.toArray(new String[0]));
        firebirdTables.setPromptText("Wybierz tabele");
    }

    public void setStage(Stage stage) {
        this.stage = stage;
    }

    public void clearAndAppendToLogArea(String message) {
        logArea.clear();
        logArea.appendText(LocalDateTime.now().toString().replaceAll("T", " ").substring(0, 19)
                + " - " + message + "\n");
    }

    public void appendToLogArea(String message) {
        logArea.appendText(LocalDateTime.now().toString().replaceAll("T", " ").substring(0, 19)
                + " - " + message + "\n");
    }
}
