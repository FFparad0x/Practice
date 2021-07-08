package com.practice.parser;
import io.javalin.Javalin;

public class Server {
    public static void main(String[] args) {
        Javalin app = Javalin.create().start(8000);
        app.config.enableWebjars();
        app.get("/", ctx -> ctx.result("df"));
    }
}