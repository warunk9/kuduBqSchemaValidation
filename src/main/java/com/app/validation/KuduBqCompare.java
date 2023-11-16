package com.app.validation;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import java.io.*;
import java.time.Instant;
import java.util.*;  


public class KuduBqCompare {

    public static void main(String[] args) {

        final String tabList = args[0];
        long epochTime = Instant.now().toEpochMilli();
        final String outputFile = "output_"+epochTime+".tsv";
        final String outputFileExcel = "output_"+epochTime+".xlsx";

        String projectId = "cfr-legacy-dev-project";

        BigQueryOptions bo = BigQueryOptions.newBuilder().setProjectId(projectId).build();
        final BigQuery bq = bo.getDefaultInstance().getService();
        List<String> bqRecord = new ArrayList<>() ;

        

        // Define Kudu master addresses
        final String[] KUDU_MASTERS = {"10.52.132.51:7051", "10.52.132.52:7051", "10.52.132.62:7051"};

        KuduClient.KuduClientBuilder clientBuilder = new KuduClient.KuduClientBuilder(String.join(",",KUDU_MASTERS));

        KuduClient kuduClient = clientBuilder.build();
        List<String> kuduRecord = new ArrayList<>() ;
        //record.add(header);

        // Provide table name
       // final String TABLE_NAME = tabNm;

        try {
            File file = new File(tabList);
            if (!file.exists() || file.isDirectory()){
                throw new FileNotFoundException("table name file not exist "+ tabList);
            }


            Map<String,String> column_dtype = new HashMap<>();

            try (BufferedReader br = new BufferedReader(new FileReader(tabList))) {
                String tableName; 
                while ((tableName = br.readLine()) != null){

                    String[] parts = tableName.split("\\.");
                    String datasetName = parts[0].toLowerCase();
                    String bqTabName = parts[1].toLowerCase();

                    String bqQuery = "SELECT column_name,data_type  FROM "+datasetName+".INFORMATION_SCHEMA.COLUMNS  WHERE lower(table_name) = '"+bqTabName+"' order by column_name;";

                    System.out.println("bqquery:"+bqQuery);
                    QueryJobConfiguration qcConfig = QueryJobConfiguration.newBuilder(bqQuery).build();

                    TableResult result = bq.query(qcConfig);

                    for (FieldValueList row : result.iterateAll()){
                        String bqColumnName = row.get("column_name").getStringValue();
                        String bqDataType = row.get("data_type").getStringValue();

                        String bqOneRec = tableName+"\t"+bqColumnName+"\t"+bqDataType;
                        bqRecord.add(bqOneRec);

                    }





                    String finalTableName = "impala::"+tableName;
                    column_dtype.clear();
                    KuduTable kuduTable = kuduClient.openTable(finalTableName);
                    
                    Schema schema = kuduTable.getSchema();
                    List<ColumnSchema> columns = schema.getColumns();
                    for (ColumnSchema column : columns) {
                        String val;
                        if(column.getTypeAttributes() != null){
                             val = column.getType().getName()+"("+column.getTypeAttributes().getPrecision()+", "+column.getTypeAttributes().getScale()+")";
                        }else{
                             val = column.getType().getName();
                        }
                        column_dtype.put(column.getName(), val);

                    }
                    Map<String, String> sortedMap = new TreeMap<>(column_dtype);
                    for (Map.Entry<String, String> entry : sortedMap.entrySet()) {
                        String oneRecKudu= finalTableName+"\t"+entry.getKey() + "\t" + entry.getValue();
                        kuduRecord.add(oneRecKudu);

                    }
                }
            }

    List<String> joinedList = concatHorizontal(bqRecord,kuduRecord);

    writeListToFile(joinedList,outputFile);

    if(bqRecord.size() != kuduRecord.size()){
        System.out.println("[ERROR]: kudu columns and Bq columns count are not same");
    }
    System.out.println("Please check validation tsv file : "+ outputFile);


    String scriptPath = new File("convert_tsv_xlsx.py").getAbsolutePath();

    System.out.println(scriptPath);



    ProcessBuilder builder = new ProcessBuilder();
    builder.command("python3", scriptPath, outputFile, outputFileExcel);
    Process process = builder.start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

    // Read the output of the Python script and print it to the console
    String line;
    while ((line = reader.readLine()) != null) {
        System.out.println(line);
    }
    try {
        process.waitFor();
        int exitValue = process.exitValue();
        if (exitValue != 0) {
            System.err.println("Error executing Python script: " + exitValue);
        }
    } catch (InterruptedException e) {
        e.printStackTrace();
    }


        
        }catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                kuduClient.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void writeListToFile(List<String> joinedList, String outFile) {
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(outFile))){
            for(String line: joinedList){
                writer.write(line);
                writer.newLine();
            }

        }catch (IOException e){
            e.printStackTrace();

        }
    }

    private static List<String> concatHorizontal(List<String> list1, List<String> list2) {

        List<String> result = new ArrayList<>();
        String header = "BQ Table Name\t"+"BQ Columns\t"+"BQ Data types\t"+"Result\t"+"Kudu Table Name\t"+"Kudu Columns\t"+"Kudu Data types";
        result.add(header);

        int maxSize = Math.max(list1.size(), list2.size());

        for(int i=0;i<maxSize;i++){
            String str1 = i< list1.size() ? list1.get(i) : "\t\t";
            String str2 = i< list2.size() ? list2.get(i) : "";
            result.add(str1+"\t\t"+str2);
        }

        return result;
    }

    private static void describeTable(KuduClient kuduClient, String tableName) throws KuduException {
        KuduTable table = kuduClient.openTable(tableName);
        Schema schema = table.getSchema();

        // Print table name
        System.out.println("Table Name: " + tableName);

        // Print column names and data types
        List<ColumnSchema> columns = schema.getColumns();
        for (ColumnSchema column : columns) {
            System.out.println("Column Name: " + column.getName() + ", Data Type: " + column.getType());
        }
    }

    private static void listRecords(KuduClient kuduClient, String tableName) throws KuduException {
        KuduTable table = kuduClient.openTable(tableName);
        KuduScanner scanner = kuduClient.newScannerBuilder(table).build();

        int count = 0;
        while (scanner.hasMoreRows() && count < 5) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext() && count < 5) {
                RowResult result = results.next();
                // Process the data, for example:
                System.out.println("Record: " + result.rowToString());
                count++;
            }
        }
    }

}
