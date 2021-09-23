import java.io.*;
import java.util.*;

import static java.lang.Thread.sleep;
import static javafx.application.Platform.exit;

/**
 * All map scripts are stored here we provides 3 mapper scripts.
 * wordCountMapper: map scripts for wordcount job
 * maxvalue: map scripts for max value job
 * orderCountMapper: map scripts for ordercount job
 * as for what these job actually do, you can find it in the design doc
 */
public class Mapper {
    /**
     * this job's file is 0 indexed
     * user defined function, i.e map script for wordcount job,
     * @param txtFilePath which txt files mapper should get
     * @param startLine which line should mapper start to parse
     * @param endLine which line should mapper stop parsing
     * @param partitionID ID of the partition that this mapper parse, used to name output file
     * @param reducerNum reducer number, used to name output file
     * @param serverId which server this script runs on
     * @return
     * @throws InterruptedException
     */
    public String workCountMapper(String txtFilePath, int startLine, int endLine, int partitionID, int reducerNum, int serverId) throws InterruptedException {
        System.out.println("startLine:" + startLine);
        System.out.println(endLine);
        Config conf = new Config();

        //if we plan to shut down this server
        //than this server will not do anything instead of sleeping
        if(serverId == conf.failWorkerId && conf.failStage == 1){
            System.out.println("Running mapping, system down, doing noting");
            sleep(10000000000L);
        }

        //cache each word in an array list
        //i.e convert each word into a tuple <word, 1>
        ArrayList<mrUtils.TwoTuple<String, Integer>> cache = new ArrayList<>();
        String filePath = txtFilePath;
        File f=new File(txtFilePath);
        FileReader fReader;
        String[] outputFilePaths = new String[reducerNum];

        //generate a file for each reducer
        for(int i = 0; i < reducerNum; i++) {
            outputFilePaths[i] = "partition-" + partitionID + "-hash-"+i +"_wordcount_result.txt";
        }

        //use each abs(word.hashcode) to put each word into different bins
        //and than write them into corresponding intermediate output file
        try {
            fReader = new FileReader(f);

            BufferedReader lineNum=new BufferedReader(fReader);
            int i = 0;
            for(;i < startLine; i++) {
                lineNum.readLine();
            }

            //convert each tuple (word, 1) into a string
            //and put them into an arraylist
            for(; i <= endLine; i++) {
                String line = lineNum.readLine();
                if(line.length()==0)
                    continue;
                String[] words = line.split(" ");
                for (String word : words) {
                    cache.add(new mrUtils.TwoTuple(word, 1));
                }
            }
        } catch (Exception e) {e.printStackTrace();}

        //write cache into file
        try{
            ArrayList<BufferedWriter> out = new ArrayList<>();
            for(String s:outputFilePaths) {
                out.add(new BufferedWriter(new FileWriter(s)));
            }
            for(mrUtils.TwoTuple t : cache) {
                String temp = "(" + t.getFirst()+", " + t.getSecond() + ")" + "\n";
                System.out.print(temp);
                System.out.print(t.getFirst().hashCode());
                out.get(Math.abs(t.getFirst().hashCode()) % reducerNum).write(temp);
            }
            for(BufferedWriter bw:out)
                bw.close();
            System.out.println("write successful");
        } catch (IOException e) {
            System.out.println("mapper file to write result to file");
        }

        //generate filename for each intermediate file
        //concat them into the following format
        //and return them to master
        //format: <filename1>&<filename2>&....
        String outputFilePath = "";
        for(int i = 0; i < outputFilePaths.length; i++) {
            outputFilePath += outputFilePaths[i];
            if(i != outputFilePaths.length-1)
                outputFilePath += "&";
        }
        return outputFilePath;
    }


    /**
     * this job's file is 1 indexed
     * user defined function, i.e map script for maxvalue job
     * this script is almost the same as wordcount job's mapper
     * therefore, no inline comment will be provides
     * @param txtFilePath which txt files mapper should get
     * @param startLine which line should mapper start to parse
     * @param endLine which line should mapper stop parsing
     * @param partitionID ID of the partition that this mapper parse, used to name output file
     * @param reducerNum reducer number, used to name output file
     * @param serverId which server this script runs on
     * @return
     * @throws InterruptedException
     */
   public String maxvalue(String txtFilePath, int startLine, int endLine, int partitionID, int reducerNum, int serverId) throws InterruptedException {
       sleep(10000L);
        System.out.println("startLine:" + startLine);
        System.out.println(endLine);
        if(startLine == 0)
            startLine = 1;
       Config conf = new Config();
       if(serverId == conf.failWorkerId && conf.failStage == 1){
           System.out.println("Running mapping, system down, doing noting");
           sleep(10000000000L);
       }
        ArrayList<mrUtils.ThreeTuple<String, String, String>> cache = new ArrayList<>();
        String filePath = txtFilePath;
        File f=new File(txtFilePath);
        FileReader fReader;
        String[] outputFilePaths = new String[reducerNum];

        for(int i = 0; i < reducerNum; i++) {
            outputFilePaths[i] = "partition-" + partitionID + "-hash-"+i +"_maxvalue_result.txt";
        }
        try {
            fReader = new FileReader(f);

            BufferedReader lineNum=new BufferedReader(fReader);
            int i = 0;
            for(;i < startLine; i++) {
                lineNum.readLine();
            }

            for(; i <= endLine; i++) {
                String line = lineNum.readLine();
                if(line.length()==0)
                    continue;
                String[] words = line.split(" ");
                cache.add(new mrUtils.ThreeTuple(words[1], words[0],words[2]));
            }
        } catch (Exception e) {e.printStackTrace();}

        try{
            ArrayList<BufferedWriter> out = new ArrayList<>();
            for(String s:outputFilePaths) {
                out.add(new BufferedWriter(new FileWriter(s)));
            }
            for(mrUtils.ThreeTuple t : cache) {
                String temp = "(" + t.getFirst()+", " + t.getSecond() + ", "+t.getThird()+")" + "\n";
                System.out.print(temp);
                System.out.print(t.getFirst().hashCode());
                out.get(Math.abs(t.getFirst().hashCode()) % reducerNum).write(temp);
            }
            for(BufferedWriter bw:out)
                bw.close();
            System.out.println("write successful");
        } catch (IOException e) {
            System.out.println("mapper file to write result to file");
        }
        String outputFilePath = "";
        for(int i = 0; i < outputFilePaths.length; i++) {
            outputFilePath += outputFilePaths[i];
            if(i != outputFilePaths.length-1)
                outputFilePath += "&";
        }
        return outputFilePath;
    }


    /**
     * this job's file is 1 indexed
     * user defined function, i.e map script for maxvalue job
     * this script is almost the same as wordcount job's mapper
     * therefore, no inline comment will be provides
     * @param txtFilePath which txt files mapper should get
     * @param startLine which line should mapper start to parse
     * @param endLine which line should mapper stop parsing
     * @param partitionID ID of the partition that this mapper parse, used to name output file
     * @param reducerNum reducer number, used to name output file
     * @param serverId which server this script runs on
     * @return
     * @throws InterruptedException
     */
    public String orderCountMapper(String txtFilePath, int startLine, int endLine, int partitionID,int reducerNum, int serverId) throws InterruptedException {
        if(startLine == 0){
            startLine = 1;
        }
        System.out.println("startLine:" + startLine);
        System.out.println(endLine);
        Config conf = new Config();
        if(serverId == conf.failWorkerId && conf.failStage == 1){
            System.out.println("Running mapping, system down, doing noting");
            sleep(10000000000L);
        }
        ArrayList<mrUtils.ThreeTuple<String, String, String>> cache = new ArrayList<>();
        String filePath = txtFilePath;
        File f=new File(txtFilePath);
        FileReader fReader;
        String[] outputFilePaths = new String[reducerNum];
        for(int i = 0; i < reducerNum; i++) {
            outputFilePaths[i] = "partition-" + partitionID + "-hash-"+i +"_ordercount_result.txt";
        }
        try {
            fReader = new FileReader(f);

            BufferedReader lineNum=new BufferedReader(fReader);
            int i = 0;
            for(;i < startLine; i++) {
                lineNum.readLine();
            }

            for(; i <= endLine; i++) {
                String line = lineNum.readLine();
                if(line.length()==0)
                    continue;
                String[] words = line.split(" ");
                cache.add(new mrUtils.ThreeTuple(words[1], words[0],words[2]));
            }
        } catch (Exception e) {e.printStackTrace();}

        try{
            ArrayList<BufferedWriter> out = new ArrayList<>();
            for(String s:outputFilePaths) {
                out.add(new BufferedWriter(new FileWriter(s)));
            }
            for(mrUtils.ThreeTuple t : cache) {
                String temp = "(" + t.getFirst()+", " + t.getSecond() + ", "+t.getThird()+")" + "\n";
                System.out.print(temp);
                System.out.print((t.getFirst().hashCode()));
                out.get(Math.abs(t.getFirst().hashCode() )% reducerNum).write(temp);
                //out.write(temp);
            }
            for(BufferedWriter bw:out)
                bw.close();
            System.out.println("write successful");
        } catch (IOException e) {
            System.out.println("mapper file to write result to file");
        }
        String outputFilePath = "";
        for(int i = 0; i < outputFilePaths.length; i++) {
            outputFilePath += outputFilePaths[i];
            if(i != outputFilePaths.length-1)
                outputFilePath += "&";
        }

        return outputFilePath;
    }




}
