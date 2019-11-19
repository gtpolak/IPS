package com.example.demo;

import javafx.application.Application;
import javafx.application.HostServices;
import javafx.application.Platform;
import javafx.stage.Stage;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

public class JavaFxApplication extends Application {

    private ConfigurableApplicationContext context;

    @Override
    public void start(Stage stage) throws Exception {
        this.context.publishEvent(new StageReadyEvent(stage));
    }

    @Override
    public void init() throws Exception {
        ApplicationContextInitializer<GenericApplicationContext> initializer =
                new ApplicationContextInitializer<GenericApplicationContext>() {
                    @Override
                    public void initialize(GenericApplicationContext genericApplicationContext) {
                        genericApplicationContext.registerBean(Application.class, () -> JavaFxApplication.this);
                        genericApplicationContext.registerBean(Parameters.class, () -> getParameters());
                        genericApplicationContext.registerBean(HostServices.class, () -> getHostServices());
                    }
                };

        this.context = new SpringApplicationBuilder()
                .sources(IPSApplication.class)
                .initializers(initializer)
                .run(getParameters().getRaw().toArray(new String[0]));
    }

    @Override
    public void stop() throws Exception {
        this.context.close();
        Platform.exit();
    }

    class StageReadyEvent extends ApplicationEvent{
        public Stage getStage(){
            return Stage.class.cast(getSource());
        }

        public StageReadyEvent(Stage source){
            super(source);
        }
    }
}
