package main.resources;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.security.auth.login.AccountException;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class Manager {
	
    private static Logger logger = Logger.getLogger(Manager.class.getName());
    private static HashMap<String, String> myAWSsqsURL;
    private static final int numOfThreads = 10;
    final   static Tag TAG_WORKER = Tag.builder().key("name").value("worker").build();
    private static LocalCloud myAWS;
    private static String result_URL;
    private static boolean keep_Alive = true;
    private static ArrayList<String> instances_Id = new ArrayList<String>();
    private static int curret_Workers = 0;
    private static Semaphore semaphore = new Semaphore(numOfThreads);
    private static String LocalAppTerminate;

    
    public static void main(String[] args){
        try {
            initLogger("ManagerLogger");
            logger.info(" Stage 1|   The Manager is running on EC2 instance\n");
            final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numOfThreads);
            List<Message> messages = new ArrayList<Message>();
            Message message;

            myAWS = new LocalCloud(false);
            myAWS.initAWSservices();
            initializeAllQueues(myAWS);
            logger.info(" Stage 2|  The Manager is listening to the queues : " + Header.INPUT_QUEUE_NAME + "\n");

            while(keep_Alive) {
                messages = getOneMessageFromSQS(myAWS, myAWSsqsURL.get(Header.INPUT_QUEUE_NAME), 0);
                while (messages.isEmpty()) {
                    messages = getOneMessageFromSQS(myAWS, myAWSsqsURL.get(Header.INPUT_QUEUE_NAME), 0);
                    if (!keep_Alive){
                        break;
                    }
                    try {Thread.sleep(Header.SLEEP_LONG);}
                    catch (InterruptedException e){logger.warning(e.toString());}
                }
                if (!keep_Alive) {
                    break;
                }
                message = messages.get(0);

                // transfer this message from the input queue to the threads queue
                myAWS.deleteSQSmessage(myAWSsqsURL.get(Header.INPUT_QUEUE_NAME), message.receiptHandle());
                myAWS.sendSQSmessage(myAWSsqsURL.get(Header.INPUT_THREAD_QUEUE_NAME), message.body());
                messages.clear();

                Runnable newTask = new Runnable() {
                    @Override
                    public void run() {
                    	try{
                        	semaphore.acquire();
                    		List<Message> messagesThread = new ArrayList<Message>();
                        	messagesThread = getOneMessageFromSQS(myAWS, myAWSsqsURL.get(Header.INPUT_THREAD_QUEUE_NAME), 180);
                            while (messagesThread.isEmpty()) {
                                messagesThread = getOneMessageFromSQS(myAWS, myAWSsqsURL.get(Header.INPUT_THREAD_QUEUE_NAME), 180);

                                try {Thread.sleep(Header.SLEEP_MID);}
                                catch (InterruptedException e){logger.warning(e.toString());}
                            }
                            Message messageCurr = messagesThread.get(0);
                            messagesThread.clear();

                            /*
                             * message = LocalAppID + " " + terminate + " " + n + " " + uploadedFileURL
                             * parsedMessage[0] = localAppID - first 12 is shortLocalAppID
                             * parsedMessage[1] = terminate - true/false
                             * parsedMessage[2] = n - number of workers
                             * parsedMessage[3] = uploadedFileURL - input file URL in S3
                             * parsedMessage[4] = bucket name - String
                             * parsedMassege[5] = key value
                             */
                            String[] parsedMessage = messageCurr.body().split(" ");
                            logger.info(" Stage 3|    The manager recive this message from LocalApp: \n");
                            logger.info("             " + messageCurr.body() + "\n");


                            int n;
                            try {
                                n = Integer.parseInt(parsedMessage[2]);
                            }
                            catch (NumberFormatException e) {
                                n = 0;
                            }
                            
                            if (n > 19) {			//make sure that the number of nodes on EC2 is not above 20 
                            	n = 19;
                            }
                            // check how many workers is currently running under the tag workers
                            curret_Workers = numberOfWorkers(myAWS);
                            // create the (n-curret_Workers) instances of Workers with the tag Worker
                            if (n - curret_Workers > 0){
                                logger.info("             Adding " + (n-curret_Workers) + " instances of Workers" + "\n");
                                instances_Id.addAll(myAWS.initEC2instance(Header.imageID,
                                        1,
                                        (n-curret_Workers),
                                        InstanceType.T2_MICRO.toString(),
                                        Header.PRE_UPLOAD_BUCKET_NAME,
                                        Header.WORKER_SCRIPT,
                                        Header.INSTANCE_WORKER_KEY_NAME,
                                        TAG_WORKER));
                                curret_Workers = numberOfWorkers(myAWS);
                            }
                            logger.info("             Thread form thread poll handling this task\n");
                            logger.info(" Stage 4|    Analyzing the following input file : " + parsedMessage[3] + "\n");
                            String[] resultAns = analyzeTextFile(myAWS, parsedMessage[0].substring(0,12), parsedMessage[4],parsedMessage[5]);

                            logger.info("\n Stage 5|    The computing is complete, the following message will send to the output queue : \n");
                            logger.info("               " + parsedMessage[0].substring(0,12) + " " + resultAns[0] + "\n");
                            myAWS.sendSQSmessage(myAWSsqsURL.get(Header.OUTPUT_QUEUE_NAME), parsedMessage[0].substring(0,12) + " " + resultAns[0] + " " + resultAns[1] + " " +resultAns[2]); //need to add bucket and key

                            // Delete the message from the thread queue
                            myAWS.deleteSQSmessage(myAWSsqsURL.get(Header.INPUT_THREAD_QUEUE_NAME), messageCurr.receiptHandle());
                            logger.info(" Stage 6|    Busy-wait to new messages..." + "\n");

                            // Check if terminate message has been received
                            if (Boolean.parseBoolean(parsedMessage[1])){
                                // Stop retrieving messages from the input queue, and wait for stopping the running
                            	LocalAppTerminate = parsedMessage[0].substring(0,12);
                                keep_Alive = false;
                            }
                            
                        }catch (AwsServiceException ase) {
                            logger.warning("Caught an AmazonServiceException, which means your request made it "
                                    + "to Amazon S3, but was rejected with an error response for some reason.");
                            logger.warning("Error Message:    " + ase.getMessage());
                            logger.warning("HTTP Status Code: " + ase.statusCode());
                            logger.warning("AWS Error Code:   " + ase.awsErrorDetails().errorCode());
                            logger.warning("Error Type:       " + "AwsServiceException");
                            logger.warning("Request ID:       " + ase.requestId());

                        } /**catch (AccountException ace) {
                            logger.warning("Caught an AmazonClientException, which means the client encountered "
                                    + "a serious internal problem while trying to communicate with S3, "
                                    + "such as not being able to access the network.");
                            logger.warning("Error Message: " + ace.getMessage());

                        }*/catch (Exception e){
                            logger.warning(e.toString());
                        } finally {
                            // if the Manager has been terminated before getting to this section this file will be
                            // uploaded in the thread that got the termination message
                            try{
                                myAWS.mUploadS3(Header.PRE_UPLOAD_BUCKET_NAME, null, Header.LOGGER_FILE_NAME, new File("ManagerLogger.txt"));
                            }catch (Exception e){
                                System.out.println("Error while uploading Manager logger + " + e.toString());
                            }
                        }
                        semaphore.release();
                    }
                };
               
                logger.info(" INFO|    Assigning a thread from thread pool to attend a new task from a Local App \n");
                // Attach a thread to handle this task
                executor.execute(newTask);

                // Wait a little - to enable AWS updating instances status
                try {Thread.sleep(Header.SLEEP_SMALL_MID);}
                catch (InterruptedException e){logger.warning(e.toString());}
            }
            logger.info(" Stage 6|    The Manager get a terminate request, terminating "+ instances_Id.size() + " workers instances..."  + "\n");
            //wait until all the workers completing their job 
            semaphore.acquire(numOfThreads); 
            // Terminate all workers instances start by this Manager
            myAWS.terminateEC2instance(instances_Id);
            // Stop the thread pool executor
            executor.shutdown();
            // Terminate the worker instances
            try {Thread.sleep(Header.SLEEP_LONG);}
            catch (InterruptedException e){logger.warning(e.toString());}
            while (numberOfWorkers(myAWS) > 0){
                instances_Id.addAll(myAWS.getEC2instancesByTagState(TAG_WORKER, "running"));
                instances_Id.addAll(myAWS.getEC2instancesByTagState(TAG_WORKER, "pending"));
                myAWS.terminateEC2instance(instances_Id);
                try {Thread.sleep(Header.SLEEP_LONG);}
                catch (InterruptedException e){logger.warning(e.toString());}
                }

                // Upload the Manager Logger file
            try{
                 myAWS.mUploadS3(Header.PRE_UPLOAD_BUCKET_NAME, null, Header.LOGGER_FILE_NAME, new File("ManagerLogger.txt"));
            }catch (Exception e){
                 System.out.println("Error has been occurred while upload the manager's logger + " + e.toString());
                }
                // Send terminate message ack to the local app that asked it
            myAWS.sendSQSmessage(myAWSsqsURL.get(Header.OUTPUT_QUEUE_NAME), Header.TERMINATED_STRING + LocalAppTerminate);
            logger.info(" Stage 7|    Manager has terminated his work" + "\n");

            
        } catch (AwsServiceException ase) {
            logger.warning("catch an AmazonServiceException, your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            logger.warning("Error Message:    " + ase.getMessage());
            logger.warning("HTTP Status Code: " + ase.statusCode());
            logger.warning("AWS Error Code:   " + ase.awsErrorDetails().errorCode());
            logger.warning("Error Type:       " + "AwsServiceException");
            logger.warning("Request ID:       " + ase.requestId());

        } /**catch (AccountException ace) {
            logger.warning("catch an AmazonClientException, the client encountered "
                    + "a internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            logger.warning("Error Message: " + ace.getMessage());
        } */catch (Exception e){
            logger.warning(e.toString());

        }
    }
    
    
      

    //inital the logger's Manager
    private static void initLogger(String logger_Name) throws IOException{
        FileHandler file_Handler = new FileHandler(logger_Name + ".txt");
        file_Handler.setFormatter(new SimpleFormatter());
        logger.setLevel(Level.ALL);
        logger.addHandler(file_Handler);
    }

    //return one message from SQS
    private static List<Message> getOneMessageFromSQS(LocalCloud myAWS, String queueURL, int TimeOut) {
    	ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
    			.maxNumberOfMessages(1)
    			.visibilityTimeout(TimeOut)
    			.queueUrl(queueURL)
    			.build();

        return myAWS.receiveSQSmessage(receiveMessageRequest);
    }
    
    //return the number of the workers
    private static int numberOfWorkers(LocalCloud myAWS) {
        return myAWS.getNumEC2instancesByTagState(TAG_WORKER, "running") +
                myAWS.getNumEC2instancesByTagState(TAG_WORKER, "pending");
    }
    
    //send the LocalApp request to the workers.
    //wait for their answer and return to LocalAPP 
    private static String[] analyzeTextFile(final LocalCloud myAWS, String shortLocalAppID, String bucket,String key){
    	String outputURL = null;
        java.util.logging.Logger
                .getLogger("org.apache.pdfbox").setLevel(java.util.logging.Level.SEVERE);

        try {
            // get the input file from the user
            File inputFile = myAWS.mDownloadS3file(bucket,key);////////////////////////////////

          //  InputStream inputStream  = inputFile.getObjectContent();
         //   BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            Reader targetReader = new FileReader(inputFile);
            BufferedReader bufferedReader = new BufferedReader(targetReader);
            logger.info("             Sending all the requests from the input file to workers \n             ");
            // Sending all input file lines to the workers input queue
            String inputLine;
            int count_Line = 0;
            while ((inputLine = bufferedReader.readLine()) != null){
                logger.info(""+count_Line);
                myAWS.sendSQSmessage(myAWSsqsURL.get(Header.INPUT_WORKERS_QUEUE_NAME), shortLocalAppID  + "\t" + inputLine);
                count_Line++;
            }
            bufferedReader.close();

            // Create new result file
            File file = new File(Header.RESULT_FILE_NAME);
            PrintWriter out = new PrintWriter(file, "UTF-8");
            List<Message> currMessages;
            logger.info("\n            Collecting results from the workers \n             ");

            // Waiting for workers to process all the requests
            while(count_Line > 0){
                currMessages = myAWS.receiveSQSmessage(myAWSsqsURL.get(Header.OUTPUT_WORKERS_QUEUE_NAME));
                for(Message message : currMessages){
                    // add this result from the worker to the Result-file
                    out.println(message.body());

                    // delete this message from the output worker queue
                    myAWS.deleteSQSmessage(myAWSsqsURL.get(Header.OUTPUT_WORKERS_QUEUE_NAME), message.receiptHandle());

                    // decrease the count - when we done processing enough messages from the workers we exit
                    count_Line--;
                    logger.info(""+count_Line);
                }
                // "busy"-wait for 0.5 second while workers keep completing other requests
                try {Thread.sleep(Header.SLEEP_SMALL);}
                catch (InterruptedException e){
                    logger.warning(e.toString());
                }
            }
            out.close();

            // Upload File file to app_bucket+LocalID S3 and return the URL
            outputURL =  myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, null, Header.RESULT_FILE_NAME, file);
        }catch (Exception e){
            logger.warning(e.toString());
        }
        String[] arr = {outputURL,Header.APP_BUCKET_NAME+shortLocalAppID,Header.RESULT_FILE_NAME};
        return arr;
    }

    //inital the queues 
    private static void initializeAllQueues(LocalCloud myAWS) {
        ArrayList<Map.Entry<String, String>> queues = new ArrayList<Map.Entry<String,String>>();
        // queue from LocalApp to Head Manager
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.INPUT_QUEUE_NAME, "0"));

        // queue from Head Manager to Manager threads
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.INPUT_THREAD_QUEUE_NAME, "0"));

        // queue from Manager to Workers
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.INPUT_WORKERS_QUEUE_NAME, "0"));

        // queue from Workers to Manager
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.OUTPUT_WORKERS_QUEUE_NAME, "0"));

        // queue from Manager to LocalApp
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.OUTPUT_QUEUE_NAME, "0"));

        myAWSsqsURL = myAWS.initSQSqueues(queues);
    }
    
    
    
}