package com.practice.parser;

import com.opencsv.exceptions.CsvException;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        File fileTransactions = new File("./source/transactions_current_datetime.csv"); // file with transactions hardcoded path will be
        File fileAverage = new File("./source/price_file_datestamp.csv"); // changed to auto finding files ASAP
        Parser parser = new Parser();
        try {
            List<String[]> result = parser.read(fileTransactions);

            parser.initDb(); //to make sure that all tables exists and create them if not
            parser.loadTransactionsToDB(result);
            result = parser.read(fileAverage);
            parser.loadAverageDataToDb(result);
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
