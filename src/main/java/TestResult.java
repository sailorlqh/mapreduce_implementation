import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.lang.System.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TestResult {
    /**
     * Thread that used to send start message to master, so the whole task can begin
     */
    public static class sendStartMessageThread extends Thread{
        public String fileName;
        public String jobName;
        public sendStartMessageThread(String file, String job){
            fileName = file;
            jobName = job;
        }
        public void run() {
            System.out.println("send message");
            cliendSendOutMessage(fileName, jobName);
        }
    }

    /**
     * deploy all servers to start task
     * @throws IOException
     */
    public static void deployMapReduceServer() throws IOException {
        System.out.println(System.getProperty("user.dir"));
        Runtime runtime = Runtime.getRuntime();
        Config conf = new Config();
        int totalWorkers = conf.getNumWorkers();
        HashMap<Integer, Integer> workerPort = conf.getWokerPort();
        Node masterNode = null;
        ArrayList<Node> wokerNodes = new ArrayList<>();
        for(int key:workerPort.keySet()) {
            String workerLogFile = "log/Worker_" + key + ".txt";
            System.out.println("Worker Server " + key + "'s log file is stored in " + workerLogFile);
            File f = new File(workerLogFile);
            ProcessBuilder pBuilder = new ProcessBuilder("java", "-cp", "target/classes", "Node", workerPort.get(key).toString(), String.valueOf(conf.Worker), String.valueOf(key));
            pBuilder.redirectOutput(f);
            pBuilder.start();
            System.out.println(key);
        }
        File masterLogFile = new File("log/Master.txt");
        System.out.println("Master Server's log file is stored in " + masterLogFile);
        ProcessBuilder masterP = new ProcessBuilder("java", "-cp", "target/classes", "Node", String.valueOf(conf.getMasterPort()), String.valueOf(conf.Master), "0");
        masterP.redirectOutput(masterLogFile);
        masterP.start();
    }

    /**
     * client send message to master
     * @param fileName type:String, file dir that mapreduce run task on
     * @param jobName type: String, which job should mapreduce do
     */
    public static void cliendSendOutMessage(String fileName, String jobName) {
        Config conf  = new Config();
        try {
            Socket socket = new Socket("localhost", conf.getMasterPort());
            OutputStream os = socket.getOutputStream();
            PrintWriter pw = new PrintWriter(os);

            String url="start/master/file=" + fileName + "/job=" + jobName;
            pw.write(url);
            pw.flush();

            socket.shutdownOutput();
            InputStream is = socket.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String info = null;
            while ((info = br.readLine()) != null) {
                System.out.println("message received:" + info);
            }
            br.close();
            is.close();
            pw.close();
            os.close();
            socket.close();
        } catch (IOException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * compare result file and the ground truth file
     * @param groundTruth type:String, groudtruth file path
     * @param result type: String, result file path
     * @param jobName type: String, job name
     * @return type:boolean, true if result file is the same as groud truth file
     */
    public static boolean compare(String groundTruth, String result, String jobName) {
        System.out.println("Comparing");
        switch(jobName){
            case "wordcount":
                System.out.println("wordcount comparing");
                return mrUtils.testTask1(groundTruth, result);
            case "ordercount":
                return mrUtils.testTask2(groundTruth, result);
//                return true;
            case "maxcount":
                return mrUtils.testTask3(groundTruth, result);
//                return true;
            default:
                System.out.println("job name is not right");
                return false;
        }
    }

    /**
     * main functio
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        mrUtils.shutDownServers();
        TimeUnit.SECONDS.sleep(5);
        Client.clientSocketHandler csh = new Client.clientSocketHandler();
        csh.start();
        int index = 0;
        String[] fileNames = new String[]{"wordcount.txt", "orders.txt", "orders.txt"};
        String[] jobNames = new String[]{"wordcount", "ordercount", "maxcount"};
        String[] correctResultFilePath = new String[]{"groudtruth_files/wordcount_result.txt", "groudtruth_files/ordercount_result.txt", "groudtruth_files/max_value_result.txt"};
        String resultFilePath;
        Config conf = new Config();
        ServerSocket ss = null;
        deployMapReduceServer();
        TimeUnit.SECONDS.sleep(1);

        sendStartMessageThread sst = new sendStartMessageThread(fileNames[index], jobNames[index]);

        sst.start();
        while(csh.fileLocation == null) {
            System.out.println("waiting for file location");
            TimeUnit.SECONDS.sleep(1);
        }
        resultFilePath = csh.fileLocation;
        if(resultFilePath.contains("fail")) {
            System.out.println("===================================================================================");
            System.out.println("Server Down, there isn't enough server left to do the job. Task Failed");
            System.out.println(resultFilePath);
            System.out.println("===================================================================================");
            java.lang.System.exit(0) ;
        }
        System.out.println("File is stored on " + resultFilePath);

        boolean ans = compare(correctResultFilePath[index], resultFilePath, jobNames[index]);
        if(ans) {
            System.out.println("===================================================================================");
            System.out.println("TEST FINISHED, RESULT IS CORRECT");
            System.out.println("===================================================================================");
        } else {
            System.out.println("===================================================================================");
            System.out.println("TEST FINISHED, RESULT IS NOT CORRECT!!!!!!!");
            System.out.println("===================================================================================");
        }
        return;

    }
}
