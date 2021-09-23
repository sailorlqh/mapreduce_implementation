import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.lang.Math;

import static java.lang.Thread.sleep;

/**
 * All reducer scripts are stored here we provides 3 reducer scripts.
 * wordcountReducer: reducer scripts for wordcount job
 * ordercountReducer: reducer scripts for max value job
 * maxvalueReducer: reducer scripts for ordercount job
 * as for what these job actually do, you can find it in the design doc
 */
public class Reducer {
    FileReader fReader;

    /**
     * User Defined Function
     * how each reducer will to their job
     * @param filePaths file paths that reducer need to parse
     * @param reducerId
     * @param hashCode
     * @return
     * @throws InterruptedException
     */
    public String wordcountReducer(ArrayList<String> filePaths, int reducerId, int hashCode) throws InterruptedException {
        // if we plan to shut down this server,
        //it will sleep and do noth
        Config conf = new Config();
        if(reducerId == conf.failWorkerId && conf.failStage == 2){
            System.out.println("Running reducing, system down, doing noting");
            sleep(10000000000L);
        }
        System.out.println("filePaths has size: ");
        System.out.println(filePaths.size());
        for(String s:filePaths) {
            System.out.println(s);
        }
        HashMap<String, Integer> result = new HashMap<>();
        String outputFilePath = "wordcount_final_result-reducerId-"+ reducerId + "-hashcode-"+ hashCode +".txt";

        //read from all files can reduce them into (word, count) pairs
        for(String filePath:filePaths) {
            try {
                BufferedReader buf = new BufferedReader(new FileReader(filePath));
                String line = buf.readLine();
                while (line != null) {
                    System.out.println(line);
                    String temp = line.substring(1, line.length() - 1);
                    System.out.println("temp + " + temp);
                    String[] vars = temp.split(", ", 0);
                    String key = vars[0];
                    System.out.println("key is: " + key);
                    System.out.println("value is " + vars[1]);
                    int count = Integer.valueOf(vars[1]);
                    if (result.containsKey(key)) {
                        System.out.println("aaaa");
                        result.put(key, result.get(key) + 1);
                    } else {
                        System.out.println("bbbb");
                        result.put(key, 1);
                    }
                    line = buf.readLine();
                }
                buf.close();
            } catch (Exception e) {
                System.out.println("phase 1");
                e.printStackTrace();
            }
        }
        System.out.println("result has size " + result.size());
        try{
            BufferedWriter out = new BufferedWriter(new FileWriter(outputFilePath));
            for(String Key : result.keySet()) {
                String temp = "(" + Key +", " + result.get(Key) + ")" + "\n";
                System.out.print(temp);
                out.write(temp);
            }
            out.close();
            System.out.println("write successful");
        } catch (IOException e) {
            System.out.println("reducer fail to write result to file");
        }
    return outputFilePath;
    }

    /**
     * User Defined Function
     * how each reducer will to their job
     * @param filePaths file paths that reducer need to parse
     * @param reducerId
     * @param hashCode
     * @return
     * @throws InterruptedException
     */
    public String ordercountReducer(ArrayList<String> filePaths, int reducerId, int hashCode) throws InterruptedException {
        Config conf = new Config();
        if(reducerId == conf.failWorkerId && conf.failStage == 2){
            System.out.println("Running reducing, system down, doing noting");
            sleep(10000000000L);
        }
        System.out.println("filePaths has size: ");
        System.out.println(filePaths.size());
        for(String s:filePaths) {
            System.out.println(s);
        }
        String outputFilePath = "ordercountcount_final_result-reducerId-"+ reducerId + "-hashcode-"+ hashCode +".txt";

        Map<String, List<String>> result = new HashMap<>();
        for(String filePath:filePaths) {
            try {
                BufferedReader buf = new BufferedReader(new FileReader(filePath));
                String line = buf.readLine();
                while (line != null) {
                    System.out.println(line);
                    String temp = line.substring(1, line.length() - 1);
                    System.out.println("temp + " + temp);
                    String[] vars = temp.split(", ", 0);
                    String key = vars[0];
                    System.out.println("key is: " + key);
                    System.out.println("value is " + vars[1] + " " + vars[2]);
                    String value = vars[1] + " " + vars[2];
                    result.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
                    line = buf.readLine();
                }
                buf.close();
            } catch (Exception e) {
                System.out.println("phase 1");
                e.printStackTrace();
            }
        }
        System.out.println("result has size " + result.size());
        try{
            BufferedWriter out = new BufferedWriter(new FileWriter(outputFilePath));
            for(String Key : result.keySet()) {
                String temp = "(" + Key +", " + result.get(Key) + ")" + "\n";
                System.out.print(temp);
                out.write(temp);
            }
            out.close();
            System.out.println("write successful");
        } catch (IOException e) {
            System.out.println("reducer fail to write result to file");
        }
        return outputFilePath;
    }

    /**
     * User Defined Function
     * how each reducer will to their job
     * @param filePaths file paths that reducer need to parse
     * @param reducerId
     * @param hashCode
     * @return
     * @throws InterruptedException
     */
     public String maxvalueReducer(ArrayList<String> filePaths, int reducerId, int hashCode) throws InterruptedException {
         Config conf = new Config();
         if(reducerId == conf.failWorkerId && conf.failStage == 2){
             System.out.println("Running reducing, system down, doing noting");
             sleep(10000000000L);
         }
        System.out.println("filePaths has size: ");
        System.out.println(filePaths.size());
        for(String s:filePaths) {
            System.out.println(s);
        }
        Map<String, Integer> result = new HashMap<>();
        String outputFilePath = "maxvalue_final_result-reducerId-"+ reducerId + "-hashcode-"+ hashCode +".txt";
         for(String filePath:filePaths) {
             System.out.println("In for, reading " + filePath);
             try {
                 BufferedReader buf = new BufferedReader(new FileReader(filePath));
                 String line = buf.readLine();
                 while (line != null) {
                     String temp = line.substring(1, line.length() - 1);
                     String[] vars = temp.split(", ", 0);
                     String key = vars[0];
                     System.out.println("key is: " + key);
                     int value = Integer.valueOf(vars[2]);

                     if (result.containsKey(key)) {
                         if (result.get(key) < value) {
                             result.put(key, value);
                         }
                     } else {
                         result.put(key, value);
                     }
                     line = buf.readLine();
                 }
                 buf.close();
             } catch (Exception e) {
                 System.out.println("phase 1");
                 e.printStackTrace();
             }
         }
        System.out.println("result has size " + result.size());
        try{
            BufferedWriter out = new BufferedWriter(new FileWriter(outputFilePath));
            for(String Key : result.keySet()) {
                String temp = "(" + Key +", " + result.get(Key) + ")" + "\n";
                System.out.print(temp);
                out.write(temp);
            }
            out.close();
            System.out.println("write successful");
        } catch (IOException e) {
            System.out.println("reducer fail to write result to file");
        }
        return outputFilePath;
    }

    public int findMaxValue(String[] vars) {
        int ans = Integer.valueOf(vars[0]);
        for(int i = 0; i < vars.length; i++) {
            if(Integer.valueOf(vars[i]) > ans)
                ans = Integer.valueOf(vars[i]);
        }
        return ans;
    }
    
//    public static void main(String[] args) {
//        Reducer r = new Reducer();
//        r.maxvalueReducerSingleMapper("partition-0_maxvalue_result.txt");
//    }
}
