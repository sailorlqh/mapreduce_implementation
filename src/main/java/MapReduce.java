import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class MapReduce {
    //deploy master and worker servers.
    public static void main(String[] args) throws IOException {
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
}
