package com.example.demo.scenesControllers;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import org.springframework.stereotype.Component;

@Component
public class MainSceneController {

    @FXML
    public Button button;

    @FXML
    public void initialize(){
        button.setOnAction( actionEvent -> System.out.println("Kliknięte"));
    }
}
