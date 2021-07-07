package com.practice.parser;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetSocketAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private static final String KEYSPACE = "practice";
    private static final String ADDRESS = "localhost";
    private static final int PORT = 9042;
    private char separator;
    private char quote;
    private String keyspace;
    private String address;
    private int port;
    private List<String[]> transactions;
    private List<String[]> prices;

    private static CqlSession session;
    public Parser(){
        this(ADDRESS, PORT, KEYSPACE);
    }
    public Parser(String address, int port, String keyspace) {
        this(address, port, keyspace,COMMA);
    }

    public Parser(String address, int port, String keyspace, char separator) {
        this(address,port,keyspace,separator, DOUBLE_QUOTES);
    }

    public Parser(String address, int port, String keyspace, char separator, char quote) {
        this.separator = separator;
        this.quote = quote;
        this.address = address;
        this.port = port;
        this.keyspace = keyspace;
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

    public void read(File transactions, File prices) throws IOException, CsvException {
        try (CSVReader csvReader = new CSVReader(new FileReader(transactions))) {
            List<String[]> data = csvReader.readAll();
            data.remove(0);
            this.transactions = data;
        }
        try (CSVReader csvReader = new CSVReader(new FileReader(prices))) {
            List<String[]> data = csvReader.readAll();
            data.remove(0);
            this.prices = data;
        }
    }
    public void initDb() {
        try{
            session = new CqlSessionBuilder().addContactPoint(new InetSocketAddress(this.address,this.port))
                    .withLocalDatacenter("datacenter1")
                    .build();
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + this.keyspace + " WITH REPLICATION = {" +
                    "'class' : 'SimpleStrategy', 'replication_factor' : 1 }; ");

            session.execute("CREATE TABLE IF NOT EXISTS " + this.keyspace + ".transactions (" +
                    "transactionId bigint," +
                    "executionEntityName text," +
                    "instrumentName text," +
                    "instrumentClassification text, " +
                    "quantity int," +
                    "price float," +
                    "currency text," +
                    "datestamp timestamp," +
                    "netAmount float," +
                    "PRIMARY KEY ((executionEntityName,instrumentName), transactionId));");
            session.execute("CREATE TABLE IF NOT EXISTS " + this.keyspace + ".prices (" +
                    "instrumentName text," +
                    "datestamp date," +
                    "currency text," +
                    "avg float," +
                    "netAmountPerDay float," +
                    "PRIMARY KEY ((datestamp, currency),instrumentName));");

        }catch (Exception e){
            e.printStackTrace();

        }
    }

    public void loadDataToDB() throws java.text.ParseException{
        for (String[] i: transactions)
        {
            PreparedStatement preparedStatement = session.prepare("INSERT INTO " + this.keyspace + ".transactions (transactionId, " +
                    "executionEntityName, instrumentName,instrumentClassification,quantity," +
                    "price,currency,datestamp,netAmount) VALUES (?,?,?,?,?,?,?,?,?);");


            try {
                Instant instant = convertTimestamp(i[7]);
                BoundStatement boundStatement = preparedStatement.bind(Long.parseLong(i[0]), i[1], i[2], i[3],
                        Integer.parseInt(i[4]), floatconv(i[5]), i[6], instant, floatconv(i[8]));
                ResultSet resultSet = session.execute(boundStatement);
            }
            catch (NumberFormatException e){
                System.out.println(e.getMessage());
            }
            catch (ParseException e){
                System.out.println("Unable to parse the date" + e.getMessage());
            }
            catch (ArrayIndexOutOfBoundsException e){
                System.out.println("input information is not matching the format ");
            }


        }
        for(String[] i : prices){
            PreparedStatement preparedStatement = session.prepare("INSERT INTO " + this.keyspace + ".prices (instrumentName, " +
                    "datestamp, currency,avg,netAmountPerDay) VALUES (?,?,?,?,?);");
            double scale = Math.pow(10,2);
            try {
                BoundStatement boundStatement = preparedStatement.bind(i[0], convertLocalDate(i[1]), i[2], floatconv(i[3]), floatconv(i[4]));
                ResultSet resultSet = session.execute(boundStatement);
            }
            catch (NumberFormatException e){
                System.out.println("Wrong number format " + e.getMessage());
            }
            catch (ParseException e){
                System.out.println(e.getMessage());
            }
            catch (ArrayIndexOutOfBoundsException e){
                System.out.println("input information is not matching the format ");
            }
        }
    }

    public Instant convertTimestamp (String in) throws ParseException{ // required by driver
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
            Date date = simpleDateFormat.parse(in);
            SimpleDateFormat newFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
            return newFormatter.parse(newFormatter.format(date)).toInstant();
    }

    public LocalDate convertLocalDate(String in) throws ParseException{ // required by driver
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy");
            Date date = simpleDateFormat.parse(in);
            SimpleDateFormat newFormatter = new SimpleDateFormat("yyyy-MM-dd");
            return newFormatter.parse(newFormatter.format(date)).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

    }

    public void Close(){
        session.close();
    }
    public float floatconv(String a) throws NumberFormatException{

        /*BigDecimal bigDecimal = new BigDecimal(a);
        bigDecimal = bigDecimal.setScale(2,RoundingMode.HALF_UP );
        return bigDecimal.floatValue();*/
        return Float.parseFloat(a);

    }

    public File getTransactionFile (String path) throws NullPointerException{
        File dir = new File(path);
        File[] files = dir.listFiles();
        for(File file : files) {
            Pattern p = Pattern.compile("transactions_current.*\\.csv");
            Matcher m = p.matcher(file.getName());
            if(m.matches())
                return file;
        }
        return null;
    }
    public File getPriceFile(String path) throws NullPointerException{
        File dir = new File(path);
        File[] files = dir.listFiles();
        for(File file : files) {
            Pattern p = Pattern.compile("price_file_date_unitimestamp.*\\.csv");
            Matcher m = p.matcher(file.getName());
            if(m.matches())
                return file;
        }
        return null;
    }

}

