package com.practice.parser;

import com.opencsv.exceptions.CsvException;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import java.util.List;

public class Main {
    public static void main(String[] args) {

        File fileTransactions = Parser.getFileByPath("./source/transactions");
        File filePrices = Parser.getFileByPath("./source/prices");
        try {Parser parser = new Parser("localhost",9042,"practice");
            parser.read(fileTransactions,filePrices);
            parser.initDb(); //to make sure that all tables exists and create them if not
            parser.loadDataToDB();
            parser.Close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
