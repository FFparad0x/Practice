package com.practice.parser;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.Date;
import java.util.List;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.opencsv.exceptions.CsvException;


public class Parser {


    private static final char COMMA = ',';
    private static final char DOUBLE_QUOTES = '"';

    private char separator;
    private char quote;

    private static CqlSession session;
    public Parser() {
        this(COMMA);
    }

    public Parser(char separator) {
        this(separator, DOUBLE_QUOTES);
    }

    public Parser(char separator, char quote) {
        this.separator = separator;
        this.quote = quote;
        session = null;
    }

    public void write(List<String[]> data, File file) throws IOException {
        try (ICSVWriter writer = new CSVWriterBuilder(
                new FileWriter(file))
                .withSeparator(separator)
                .withQuoteChar(quote)
                .build()) {
            writer.writeAll(data);

        }
    }

    public List<String[]> read(File file) throws IOException, CsvException {
        try (CSVReader csvReader = new CSVReader(new FileReader(file))) {
            List<String[]> data = csvReader.readAll();
            data.remove(0);
            return data;
        }
    }
    public void initDb() {
        try{
            session = new CqlSessionBuilder().addContactPoint(new InetSocketAddress("localhost",9042))
                    .withLocalDatacenter("datacenter1")
                    .build();
            session.execute("CREATE KEYSPACE IF NOT EXISTS practice WITH REPLICATION = {" +
                    "'class' : 'SimpleStrategy', 'replication_factor' : 1 }; ");

            //session.execute( "use practice; ");

            session.execute("CREATE TABLE IF NOT EXISTS practice.transactions (" +
                    "transactionId bigint," +
                    "executionEntityName text," +
                    "instrumentName text," +
                    "instrumentClassification text, " +
                    "quantity int," +
                    "price float," +
                    "currency text," +
                    "datestamp timestamp," +
                    "netAmount double," +
                    "PRIMARY KEY ((executionEntityName,instrumentName), transactionId));");
            session.execute("CREATE TABLE IF NOT EXISTS practice.average (" +
                    "instrumentName text," +
                    "datestamp date," +
                    "currency text," +
                    "avg float," +
                    "netAmountPerDay double," +
                    "PRIMARY KEY ((datestamp, currency),instrumentName));");

        }catch (Exception e){
            e.printStackTrace();

        }
    }

    public void loadTransactionsToDB(List<String[]> data) throws java.text.ParseException{
        for (String[] i: data)
        {
            PreparedStatement preparedStatement = session.prepare("INSERT INTO practice.transactions (transactionId, " +
                    "executionEntityName, instrumentName,instrumentClassification,quantity," +
                    "price,currency,datestamp,netAmount) VALUES (?,?,?,?,?,?,?,?,?);");

            Instant instant = convertTimestamp(i[7]);

            BoundStatement boundStatement = preparedStatement.bind(Long.parseLong(i[0]),i[1],i[2],i[3],
                    Integer.parseInt(i[4]),Float.parseFloat(i[5]),i[6],instant,Double.parseDouble(i[8]));
            ResultSet resultSet = session.execute(boundStatement);

        }
    }
    public void loadAverageDataToDb(List<String []> data){ //load all data 1 by 1
        for (String[] i: data)
        {
            PreparedStatement preparedStatement = session.prepare("INSERT INTO practice.average (instrumentName, " +
                    "datestamp, currency,avg,netAmountPerDay) VALUES (?,?,?,?,?);");
            BoundStatement boundStatement = preparedStatement.bind(i[0], convertLocaldate(i[1]), i[2], Float.parseFloat(i[3]),Double.parseDouble(i[4]));
            ResultSet resultSet = session.execute(boundStatement);

        }
    }
    public Instant convertTimestamp (String in){ // required by driver
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
        try {
            Date date = simpleDateFormat.parse(in);
            SimpleDateFormat newFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
            return newFormater.parse(newFormater.format(date)).toInstant();
        } catch (ParseException e) {
            e.printStackTrace();
        }
       return null;
    }

    public LocalDate convertLocaldate(String in){ // required by driver
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy");
        try {
            Date date = simpleDateFormat.parse(in);
            SimpleDateFormat newFormater = new SimpleDateFormat("yyyy-MM-dd");
            return newFormater.parse(newFormater.format(date)).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void Close(){
        session.close();
    }

}

