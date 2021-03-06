package main.resources;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import javax.security.auth.login.AccountException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.internal.AwsErrorCode;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Tag;

import software.amazon.awssdk.services.sqs.model.Message;


/**
 * Distributed System Programming : Cloud Computing and Map-Reducce1 - 2019/Spring
 * Assignment 1
 *
 * DSP Local Application
 * PDF Document Conversion in the Cloud
 *
 *
 * LocalApp class - represent the Local Application
 * overwriteScript, overwriteJars to upload new jars\scripts to pre-upload bucket.
 */
public class LocalApp {
	
	final static Tag TAG_MANAGER = Tag.builder()
	        .key("name")
	        .value("manager")
	        .build();
    private static String LocalAppID;
    private static String shortLocalAppID;

    public static void main( String[] args ){

        java.util.logging.Logger
                .getLogger("com.amazonaws.util.Base64").setLevel(Level.OFF);

        // General vars
        String inputFileName = args[0];
        String outputFileName = args[1];
        int n = Integer.parseInt(args[2]);
        boolean terminate = false;

        boolean overwriteScript = false;
        boolean overwriteJars = false;

        /** 1. if you want to terminate the manager args = inputFileName outputFileName n terminate */
        if (args.length > 3 && args[3].equals("terminate"))
            terminate = true;

        // Promotion
        System.out.println("****************************************************************");
        System.out.println(" Distriduted System Programming : PDF Document Conversion in the Cloud");
        System.out.println(" By Maor Assayag & Refahel Shetrit \n");
        System.out.println("\n Stage 1|    Local AWS App has been started \n");

        // Initialize LocalCloud object and get a random UUID
        LocalAppID = UUID.randomUUID().toString();
        shortLocalAppID = LocalAppID.substring(0, 12); // used for uniq bucket name for each LocalApp
        LocalCloud myAWS = new LocalCloud(true);
        myAWS.initAWSservices();
        String managerID;
        try {
            /**2. Start a Manager instance on EC2 (if its not already running) */
            String[] results = checkManager(myAWS);
            if (results[0] != null) {
                managerID = results[0];
                // Promotion of running Manager
                System.out.println("\n Stage 2|    Manager instance already running, Manager ID : " + managerID + "\n");

            } else if (results[2] != null) {
                managerID = results[2];
                // Promotion of pending Manager
                System.out.println("\n Stage 2|    Manager instance already pending, Manager ID : " + managerID + "\n");

            }else {
                managerID = results[1];
                Boolean restartResult = false;
                if (managerID !=null) {
                    restartResult = myAWS.restartEC2instance(managerID);}

                if (restartResult){
                    // Promotion of rebooted Manager
                    System.out.println("\n Stage 2|    Manager instance has been rebooted, Manager ID : " + managerID + "\n");

                } else{
                    managerID = startManager(myAWS, overwriteScript, overwriteJars);
                    // Promotion of new Manager
                    System.out.println("             Manager instance has been started, Manager ID : " + managerID + "\n");
                }
            }

            /** 3. Upload the input file to this LocalApp S3 bucket in folder INPUT_FOLDER_NAME*/
            //myAWS.mCreateFolderS3(Header.APP_BUCKET_NAME + shortLocalAppID, Header.INPUT_FOLDER_NAME);
            String uploadedFileURL = uploadFileToS3(myAWS, inputFileName, Header.INPUT_FOLDER_NAME);
            System.out.println("\n Stage 3|    The input file has been uploaded to " + uploadedFileURL + "\n");


            /** 4. Send the uploaded file URL to the SQS queue*/
            // How many lines in input-file ?
            int numOfLines = 0;
            try {
                FileReader       input = new FileReader(inputFileName);
                LineNumberReader count = new LineNumberReader(input);
                while (count.skip(Long.MAX_VALUE) > 0) {}
                numOfLines = count.getLineNumber() + 1;
            }catch (Exception e){
                e.printStackTrace();
            }
            System.out.println("             Counted " + numOfLines + " lines in the input-file \n");

            int numOfWorkers = 1;
            if (n != 0 && numOfLines != 0){
                numOfWorkers = numOfLines / n ;
            }
            if (numOfWorkers > 19){
                n = 19;
                System.out.println("             Free-tire on EC2 supports up to 19 T2.micro instances of workers, requesting 19\n");
            } else{
                n = numOfWorkers;
            }
            System.out.println("             Requesting " + n + " Workers to handle this request, each one suppose to handle " + (numOfLines/n) +" PDF's\n");

            String msg = LocalAppID + " " + terminate + " " + n + " " + uploadedFileURL;  //need to add bucket and key
            send2SQS(myAWS, msg);
            if (terminate){
                System.out.println("\n Stage 4|    Request with terminate message has been sent to the input queue with the following message : \n");
                System.out.println("             " + msg + "\n");
            }else{
                System.out.println("\n Stage 4|    The file URL has been sent to the SQS queue with the following message : \n");
                System.out.println("             " + msg + "\n");
            }

            /** 5. Wait & Receive the response from the Manager instance for the operation that has been requested*/
            System.out.println("\n Stage 5|    Waiting for response from the Manager on queue " + Header.OUTPUT_QUEUE_NAME + " ... \n");
            String[] resultURL = waitForAnswer(myAWS, shortLocalAppID, 500);
            System.out.println("             Response from the Manager is ready on : "+ resultURL + "\n");


            /** 6. Download the operation summary file from S3 & Create an HTML file representing the results*/
            String outputFilePath = downloadResult(myAWS, resultURL, outputFileName);
            System.out.println("\n Stage 6|    Summary file received in the Local AWS App \n");
            System.out.println("             HTML file representing the results has been created localy on : " + outputFilePath + "\n");


            /** 7. Send a terminate message to the manager if it received terminate as one of the input arguments*/
            if (terminate) {
                endManager(myAWS, managerID, Header.SLEEP_MID);
                System.out.println("\n Stage 7|    Manager has been terminated as requested \n");
                System.out.println("\n Stage 8|    Local AWS App finished with terminating the Manager");
            }else
                System.out.println("\n Stage 7|    Local AWS App finished without terminating the Manager");

            System.out.println(" _______________   __________ \n" +
                    " ___  ____/___  | / /___  __ \\\n" +
                    " __  __/   __   |/ / __  / / /\n" +
                    " _  /___   _  /|  /  _  /_/ / \n" +
                    " /_____/   /_/ |_/   /_____/");
            System.out.println("****************************************************************\n");

        } catch (AwsServiceException e) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + e.getMessage());
            System.out.println("HTTP Status Code: " + e.statusCode());
            System.out.println("Error Type:       " + "AwsServiceException");
            System.out.println("AWS Error Code:   " + e.awsErrorDetails().errorCode());
            System.out.println("Request ID:       " + e.requestId());
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");

        } catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * startManager - method used to start a manager if a manager isn't active
     * Recommended AMI image which supports user-data : ami-51792c38
     * Currently using T2 instances are Burstable Performance Instances that provide
     * a baseline level of CPU performance with the ability to burst above the baseline.
     *
     * Image ID : ami-0080e4c5bc078760e - Linux 64 bit with full support of java
     * @param myAWS LocalCloud object
     * @param overwriteJars doest we want to overwrite the jars on pre-upload bucket ?
     * @param overwriteScript doest we want to overwrite the scripts on pre-upload bucket ?
     * @return the id of a manager instance that has been created
     */
    private static String startManager(LocalCloud myAWS, boolean overwriteScript, boolean overwriteJars) {
        uploadScripts(myAWS, overwriteScript);
        uploadJars(myAWS, overwriteJars);
        ArrayList<String> managerInstance = myAWS.initEC2instance(Header.imageID,
                1, 1, InstanceType.T2_MICRO.toString(), Header.PRE_UPLOAD_BUCKET_NAME,
                Header.MANAGER_SCRIPT, Header.INSTANCE_MANAGER_KEY_NAME, TAG_MANAGER);
        return 	managerInstance.get(0);
    }

    /**
     * checkManager - method used to determined if the local app needs to start a new Manager
     * instance in aws. Checking if a Manager instance is running or stopped.
     *
     * @param myAWS LocalCloud amazon web service object with EC2, S3 & SQS
     * @return instanceID if manager found, else null
     */
    private static String[] checkManager(LocalCloud myAWS) {
        String[] results = new String[3];
        results[0] = myAWS.getEC2instanceID(TAG_MANAGER, "running");
        results[1] = myAWS.getEC2instanceID(TAG_MANAGER, "pending");
        results[2] = myAWS.getEC2instanceID(TAG_MANAGER, "stopped");
        return results;
    }

    /**
     * uploadFileToS3 - method that uploads the input file from the user to be read, distributed &
     * executed by a running manager.
     *
     * @param inputFileName location of the file to upload to S3
     * @param myAWS LocalCloud amazon web service object with EC2, S3 & SQS
     * @param folder which folder in local app bucket
     * @return path (url) of the uploaded file in S3 - a confirmation of successful upload
     */
    private static String uploadFileToS3(LocalCloud myAWS, String inputFileName, String folder) {
        return myAWS.mUploadS3(Header.APP_BUCKET_NAME + shortLocalAppID, folder, Header.INPUT_FILE_NAME, new File(inputFileName));
    }

    /**
     * send2SQS - method that send a message to Amazon Simple Queue Service :
     * A Fully managed message queues for microservices, distributed systems, and serverless applications.
     * The method initialize a Queue of messages (if needed) and send a message with the input file's URL and
     * information about the number of workers, LocalAppID etc.
     *
     * @param myAWS LocalCloud amazon web service object with EC2, S3 & SQS
     * @param msg the message to be sent
     */
    private static void send2SQS(LocalCloud myAWS, String msg) {
        String queueURL = myAWS.initSQSqueues(Header.INPUT_QUEUE_NAME, "0");
        myAWS.sendSQSmessage(queueURL, msg);
    }

    /**
     * waitForAnswer - method thats check the SQS in the cloud until there's a message
     * associate with the LocalAppID (meaning the Manager has responded and finished/stop
     * processing the requested operation)
     * Blocking-IO method that sleeps for 'sleep' ms
     *
     * @param myAWS LocalCloud amazon web service object with EC2, S3 & SQS
     * @param key UUID key associate with this instance of Local application (global LocalAppId)
     * @param sleep the amount of time in ms between searching for answer in the SQS
     */
    private static String[] waitForAnswer(LocalCloud myAWS, String key, int sleep) {
        List<Message> messages;
        String resultURL = null;
        String bucketAns = null;
        String keyAns = null;
        String[] ans = {null,null,null};
        String queueUrl = myAWS.initSQSqueues(Header.OUTPUT_QUEUE_NAME, "0");

        while(true) {
            messages = myAWS.receiveSQSmessage(queueUrl); // Receive List of all messages in queue
            for (Message message : messages) {
                String[] msg = message.body().split(" ");
                if(msg[0].equals(key)) {
                    String myMessage = message.receiptHandle();
                    // the terminate message have only msg[0]
                    if (msg.length > 1){
                        resultURL = msg[1];
                        bucketAns = msg[2];
                        keyAns	  = msg[3];
                    }
                    myAWS.deleteSQSmessage(Header.OUTPUT_QUEUE_NAME, myMessage); // Delete the message from the queue
                    ans[0] = resultURL;
                    ans[1] = bucketAns;
                    ans[2] = keyAns;
                    return ans;
                }
            }
            // busy-wait
            try {Thread.sleep(sleep);}
            catch (InterruptedException e){
                e.printStackTrace();
                return ans ;
            }
        }
    }

    /**
     * downloadResult - method that downloads the summary file from S3 for creating
     * an html file representing the results.
     *
     * @param myAWS LocalCloud amazon web service object with EC2, S3 & SQS
     * @param resultsURL the returned URL from the Manager
     * @param outputFileName the local result HTML
     * @return result - a list of lines(String) from the format:
     *                  '<operation>: input file output file'
     */
    private static String downloadResult(LocalCloud myAWS, String[] results_strings, String outputFileName) {
        String outputFilePath = null;
        try {
            // get the result file from S3
            File resultFile = myAWS.mDownloadS3file(results_strings[1], results_strings[2]); // bucket, key
            Reader targetReader = new FileReader(resultFile); //conversion
            BufferedReader bufferedReader = new BufferedReader(targetReader);
            String line;

            // create the HTML file
            PrintWriter out = new PrintWriter(outputFileName + ".HTML", "UTF-8");
            out.println("<html>\n");
            out.println("<pre style=\"float: top;\" contenteditable=\"false\">_____/\\\\\\\\\\\\\\\\\\______/\\\\\\______________/\\\\\\______/\\\\\\\\\\\\\\\\\\\\\\___\n" +
                    " ___/\\\\\\\\\\\\\\\\\\\\\\\\\\___\\/\\\\\\_____________\\/\\\\\\____/\\\\\\/////////\\\\\\_\n" +
                    "  __/\\\\\\/////////\\\\\\__\\/\\\\\\_____________\\/\\\\\\___\\//\\\\\\______\\///__\n" +
                    "   _\\/\\\\\\_______\\/\\\\\\__\\//\\\\\\____/\\\\\\____/\\\\\\_____\\////\\\\\\_________\n" +
                    "    _\\/\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\___\\//\\\\\\__/\\\\\\\\\\__/\\\\\\_________\\////\\\\\\______\n" +
                    "     _\\/\\\\\\/////////\\\\\\____\\//\\\\\\/\\\\\\/\\\\\\/\\\\\\_____________\\////\\\\\\___\n" +
                    "      _\\/\\\\\\_______\\/\\\\\\_____\\//\\\\\\\\\\\\//\\\\\\\\\\_______/\\\\\\______\\//\\\\\\__\n" +
                    "       _\\/\\\\\\_______\\/\\\\\\______\\//\\\\\\__\\//\\\\\\_______\\///\\\\\\\\\\\\\\\\\\\\\\/___\n" +
                    "        _\\///________\\///________\\///____\\///__________\\///////////_____\n" +
                    "</pre>");
            out.println("    <h2>Distriduted System Programming : PDF Document Conversion in the Cloud</h2>\n" +
                    "    <h3>By Maor Assayag & Refahel Shetrit</h3>\n" +
                    "    <h3>Results of LocalApp ID : " + LocalAppID + "</h3> <br>");
            out.println("<body>");

            while ((line = bufferedReader.readLine()) != null)
                out.println(line + "<br>");
            bufferedReader.close();

            out.println("</body>\n</html>");
            outputFilePath = new java.io.File(".").getCanonicalPath() + File.separator + outputFileName + ".html";
            out.close();

        } catch (Exception ex){
            ex.printStackTrace();
        }

        //aws.mDeleteS3file(Header.APP_BUCKET_NAME + shortLocalAppID, Header.RESULT_FILE_NAME);
        return outputFilePath;
    }

    /**
     * endManager - terminate the Manager Instance on EC2 service
     *
     * @param myAWS LocalCloud amazon web service object with EC2, S3 & SQS
     * @param managerID - EC2 Manger instance ID
     * @param sleep the amount of time in ms between searching for answer in the SQS
     */
    private static void endManager(LocalCloud myAWS, String managerID, int sleep) {
        // wait for termination message from the Manager
        waitForAnswer(myAWS, Header.TERMINATED_STRING + shortLocalAppID, sleep);

        // terminate the manager
        myAWS.terminateEC2instance(managerID);
    }

    /**
     * uploadScripts - upload the scripts to the pre-upload bucket on S3
     *
     * @param myAWS LocalCloud amazon web service object with EC2, S3 & SQS
     * @param overwrite doest we want to overwrite the scripts on pre-upload bucket ?
     */
    private static void uploadScripts(LocalCloud myAWS, boolean overwrite) {
        System.out.println("\n Stage 2|    Uploading files (scripts & jars) to the general Bucket..." + "\n");

        if (/**TODO!myAWS.doesFileExist(Header.PRE_UPLOAD_BUCKET_NAME, Header.MANAGER_SCRIPT) ||*/ overwrite){

            File managerScriptFile = new File("C:\\Users\\MaorA\\IdeaProjects\\DSP\\src\\scriptManager.txt");
            String path = myAWS.mUploadS3(Header.PRE_UPLOAD_BUCKET_NAME, null, Header.MANAGER_SCRIPT, managerScriptFile);
            System.out.println("             Manager script has been uploaded to " + path + "\n");

            File workerScriptFile = new File("C:\\Users\\MaorA\\IdeaProjects\\DSP\\src\\scriptWorker.txt");
            String path2 = myAWS.mUploadS3(Header.PRE_UPLOAD_BUCKET_NAME, null, Header.WORKER_SCRIPT, workerScriptFile);
            System.out.println("             Worker script has been uploaded to " + path2 + "\n");

        }else{
            System.out.println("             Manager script was not overwrite as configured" + "\n");
            System.out.println("             Worker script was not overwrite as configured" + "\n");
        }
    }

    /**
     * uploadJars - upload the jars of Worker.java & ManagerApp.java to the pre-upload bucket on S3
     * @param myAWS LocalCloud amazon web service object with EC2, S3 & SQS
     * @param overwrite doest we want to overwrite the jars on pre-upload bucket ?
     */
    private static void uploadJars(LocalCloud myAWS, boolean overwrite) {
        if (/**TODO!myAWS.doesFileExist(Header.PRE_UPLOAD_BUCKET_NAME, Header.MANAGER_JAR) ||*/ overwrite){

            File managerFile = new File("C:\\Users\\MaorA\\IdeaProjects\\DSP\\out\\artifacts\\ManagerApp_jar\\ManagerApp.jar");
            String path = myAWS.mUploadS3(Header.PRE_UPLOAD_BUCKET_NAME, null, Header.MANAGER_JAR, managerFile);
            System.out.println("             Manager.jar has been uploaded to " + path + "\n");

            File workerFile = new File("C:\\Users\\MaorA\\IdeaProjects\\DSP\\out\\artifacts\\Worker_jar\\Worker.jar");
            String path2 = myAWS.mUploadS3(Header.PRE_UPLOAD_BUCKET_NAME, null, Header.WORKER_JAR, workerFile);
            System.out.println("             Worker.jar has been uploaded to " + path2 + "\n");

        }else{
            System.out.println("             Manager jar was not overwrite as configured" + "\n");
            System.out.println("             Worker jar was not overwrite as configured" + "\n");
        }
    }
}

