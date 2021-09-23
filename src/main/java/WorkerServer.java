import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


public class WorkerServer {
    static int role; //1: mapper, 2: reducer
    static int lock = 0;
    public static Config conf;
    private static int id;
    private static String job;
    public WorkerServer(int id) {
        conf = new Config();
        this.id = id;
    }


    public int getWorkerId() {
        return this.id;
    }

    /**
     * when mapper finished its job, send file name to master
     * @param filePath file's path that mapper generates
     * @return message status
     */
    public static String sendFinishMessageToMaster(String filePath) {
        String address = "mapper-task-finish/id=" + id + "/file=" + filePath;
        return mrUtils.sendMessage(conf.getMasterPort(), address);
    }

    /**
     * when reducer finished its job, send file name to master
     * @param filePath file's path that reducer generates
     * @return message status
     */
    public static String sendReducerFinishMessageToMaster(String filePath) {
        String address = "reducer-task-finish/id=" + id + "/file=" + filePath;
        return mrUtils.sendMessage(conf.getMasterPort(), address);
    }

    /**
     * mapper calling UDF's mapping, start doing mapping job
     * @param filePath which file to do mapping on
     * @param startLine partition's start line
     * @param endLine partition's end line
     * @param partitionId
     * @return message status
     * @throws InterruptedException
     */
    public static String startMapPhase(String filePath, int startLine, int endLine, int partitionId) throws InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss a");
        Date date = new Date();// 获取当前时间
        System.out.println("Mapper start to map " + sdf.format(date));
        Mapper m = new Mapper();
        String resultFilePath;
        switch(job) {
            case "wordcount":
                resultFilePath = m.workCountMapper(filePath, startLine, endLine, partitionId, conf.getNumReducer(), id);
                break;
            case "ordercount":
                resultFilePath = m.orderCountMapper(filePath, startLine, endLine, partitionId, conf.getNumReducer(), id);
                break;
            case "maxcount":
                resultFilePath = m.maxvalue(filePath, startLine, endLine, partitionId, conf.getNumReducer(), id);
                break;
            default:
                resultFilePath = m.workCountMapper(filePath, startLine, endLine, partitionId, conf.getNumReducer(), id);
                // code block
        }
        return sendFinishMessageToMaster(resultFilePath);
    }

    /**
     * when receives master assignrole message, this funciton will be called
     * init this worker's role and job name
     * @param inputRole
     * @param inputJob
     * @return message status
     */
    public static String assignWorkerRole(String inputRole, String inputJob) {
        job = inputJob;
        if(inputRole.equals("mapper")) {
            role = 0;
        } else {
            if(inputRole.equals("reducer")) {
                role = 1;
            } else {
                return "your input is invalid";
            }
        }
        System.out.println("assigned role: " + role);
        return "role assigned success";
    }

    /**
     * start doing reducing
     * @param filePath which files should reducer do reduce on
     * @param hashCode
     * @return
     * @throws InterruptedException
     */
    public static String startReducePhase(String filePath, int hashCode) throws InterruptedException {
        System.out.println("Message Recevied, Start Reduce Phase");
        Reducer r = new Reducer();
        String resultFilePath = "no-job-found";
        String[] temp = filePath.split("&", 0);
        ArrayList<String> allFilePath = new ArrayList<>();
        for(String s:temp) {
            System.out.println(s);
            allFilePath.add(s);
        }
        switch(job) {
            case "wordcount":
                resultFilePath = r.wordcountReducer(allFilePath, id, hashCode);
                // code block
                break;
            case "ordercount":
                resultFilePath = r.ordercountReducer(allFilePath, id, hashCode);
                break;
            case "maxcount":
                resultFilePath = r.maxvalueReducer(allFilePath, id, hashCode);
                break;
            default:
                resultFilePath = r.wordcountReducer(allFilePath, id, hashCode);
                // code block
        }
        return sendReducerFinishMessageToMaster(resultFilePath);
    }


//    public static void main(String[] args) {
//        int tempid = 1;
//        Config conf = new Config();
//        WorkerServer ws = new WorkerServer(tempid);
//        int server_port = conf.getWorkertPort(ws.getWorkerId());
//        port(server_port);
//        System.out.println("Worker Server Running on Port " + server_port);
//
//        get("/assignrole/:role", (request, response) -> {
//            if(request.params(":role").equals("mapper")) {
//               role = 0;
//            }
//            else {
//                if(request.params(":role").equals("reducer")) {
//                    role = 1;
//                }
//                else {
//                    return "your input role type is not correct";
//                }
//            }
//            System.out.println("assigned role: " + role);
//            return "role assigned success";
//        });
//
//
//
//        get("/start/:filePath/:jarPath/:startLine/:endLine/:partitionId", (request, response) -> {
//            if(lock == 0) {
//                lock = 1;
//                String filePath = request.params(":filePath").split("=", 0)[1];
//                String jarPath = request.params(":jarPath").split("=", 0)[1];
//                int startLine = Integer.parseInt(request.params(":startline").split("=", 0)[1]);
//                int endLine = Integer.parseInt(request.params(":endLine").split("=", 0)[1]);
//                int partitionId = Integer.parseInt(request.params(":partitionId").split("=", 0)[1]);
//                System.out.print(filePath + " " + jarPath + " " + startLine + " " + endLine);
//                Mapper m = new Mapper();
//                String resultFilePath = m.workCountMapper(filePath, startLine, endLine, partitionId);
//                sendFinishMessageToMaster(resultFilePath);
//
//            }
//            return true;
//        });
//
//        get("startReduce/:mapperId/:filePath", (request, response) -> {
//            if(lock == 0) {
//                int mapperId = Integer.valueOf(request.params(":mapperId").split("=", 0)[1]);
//                String filePath = request.params(":filePath").split("=", 0)[1];
//                Reducer r = new Reducer();
//                String result = r.wordcountReducerSingleMapper(filePath);
//                return result;
//            }
//            return "server is in use";
//        });
//    }
}
