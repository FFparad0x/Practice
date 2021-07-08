package com.practice.parser;

import com.opencsv.exceptions.CsvException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;

public class Main {
    public static void main(String[] args) {


        try {Parser parser = new Parser("localhost",9042,"practice");
            File fileTransactions = parser.getTransactionFile("./source/transactions");
            File filePrices = parser.getPriceFile("./source/prices");
            parser.read(fileTransactions,filePrices);
            parser.initDb(); //to make sure that all tables exists and create them if not
            parser.loadDataToDB();
            parser.quickAnalyze("./output/");
            parser.Close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (NullPointerException e){
            e.printStackTrace();
            System.out.println("hello, give me file");
        }


    }
}
