package com.example.demo;

import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import javafx.util.Callback;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;

@Component
public class StageInitializer implements ApplicationListener<JavaFxApplication.StageReadyEvent> {

    private final String appTitle;
    private final Resource fxml;
    private final ApplicationContext applicationContext;

    public StageInitializer(@Value("${spring.application.ui.title}") String appTitle,
                            @Value("classpath:/ui.fxml") Resource resource, ApplicationContext applicationContext) {
        this.appTitle = appTitle;
        this.fxml = resource;
        this.applicationContext = applicationContext;
    }

    @Override
    public void onApplicationEvent(JavaFxApplication.StageReadyEvent stageReadyEvent) {
        try {
            Stage stage = stageReadyEvent.getStage();
            URL url = this.fxml.getURL();
            FXMLLoader loader = new FXMLLoader(url);
            loader.setControllerFactory(applicationContext::getBean);
            Parent root = loader.load();
            Scene scene = new Scene(root, 500, 500);
            stage.setScene(scene);
            stage.setTitle(appTitle);
            stage.show();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
