package core;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Spring boot 入口启动程序
 * Created by xuwei
 */
@SpringBootApplication //定义springboot入口程序
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class,args);
    }
}
