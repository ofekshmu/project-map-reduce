package main.resources;

import java.io.*;
import java.net.URL;
import java.util.*;

import javax.security.auth.login.AccountException;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class Workers {
	
    private static int index;
    private static String workerID;
    private static HashMap<String, String> myAWSsqsURL;
    
    public static void main(String[] args){

        //System.out.println(" Stage 1|    Worker App has been started on EC2 instance\n");
        List<Message> messages = new ArrayList<Message>();
        Message currMessage;
        String outputMessage;
        index = 0;
        workerID = UUID.randomUUID().toString().substring(0, 12);

        try {
            /** 1. Init queues, get queues URLs*/
            LocalCloud myAWS = new LocalCloud(false);
            myAWS.initAWSservices();
            initializeAllQueues(myAWS);
            //System.out.println(" Stage 2|    Start listening to the following queue : " + Header.OUTPUT_QUEUE_NAME + "\n");

            /** 2. The worker will keep running until the Manager will terminate him upon receiving terminate message*/
            while(true) {

                /** 3. Try to get 1 message from the SQS input queue (messages sent by the Manager)*/
                while (messages.isEmpty()) {
                    messages = getOneMessageFromSQS(myAWS, myAWSsqsURL.get(Header.INPUT_WORKERS_QUEUE_NAME));

                    try {Thread.sleep(Header.SLEEP_SMALL_MID);}
                    catch (InterruptedException e){e.printStackTrace();}
                }

                /** 4. We pool 1 message from the workers queue that is invisible from everyone else */
                currMessage = messages.get(0);
                messages.clear();

                //System.out.println(" Stage 3|    Parsing the following message : \n");
                //System.out.println("             " + currMessage.getBody() + "\n");
                //System.out.println(" Stage 4|    Analyzing the following input file : " + parsedMessage[3] + "\n");
                /** 4. Analyze the message and convert the PDF according to the operation */
                outputMessage = analyzeMessage(myAWS, currMessage);

                if (outputMessage != null){
                    index++;
                    //System.out.println("\n Stage 5|    Computing complete, Sending the following message to the output queue : \n");
                    //System.out.println("               " + outputMessage +"\n");
                    /** 5. Send the result to the Manager */
                    myAWS.sendSQSmessage(myAWSsqsURL.get(Header.OUTPUT_WORKERS_QUEUE_NAME), outputMessage);

                    /** 6. Delete the message from the workers queue */
                    myAWS.deleteSQSmessage(myAWSsqsURL.get(Header.INPUT_WORKERS_QUEUE_NAME), currMessage.receiptHandle());
                    //System.out.println(" Stage 6|    Busy-wait to new messages..." + "\n");
                }
            }
        } catch (AwsServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.statusCode());
            System.out.println("Error Type:       " + "AwsServiceException");
            System.out.println("AWS Error Code:   " + ase.awsErrorDetails().errorCode());
            System.out.println("Request ID:       " + ase.requestId());
        }/** catch (AccountException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }*/ catch (Exception e){
            e.printStackTrace();
        }
    }

    private static List<Message> getOneMessageFromSQS(LocalCloud myAWS, String queueURL) {
    	
    	ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
    			.queueUrl(queueURL)
    			.maxNumberOfMessages(1)
    			.visibilityTimeout(10)
    			.build();

        return myAWS.receiveSQSmessage(receiveMessageRequest);
    }
    
    
    private static String convertPDF(LocalCloud myAWS, String shortLocalAppID, String operation, String pdfURL) {
        String outputLine = operation + ":" + "\t" + pdfURL + "\t";
        try {
            // Load PDF from URL
            PDDocument pddDocument = PDDocument.load(new URL(pdfURL));
            if (!pddDocument.isEncrypted()) {
                if (operation.equals("ToText")){
                    // ToText - convert the first page of the PDF file to a text file.
                    PDFTextStripper textStripper = new PDFTextStripper();

                    // Extract the first page of the PDF
                    textStripper.setStartPage(1);
                    textStripper.setEndPage(1);
                    String firstPage = textStripper.getText(pddDocument);

                    // Create new file
                    File file = new File(Header.TEXT_NAME + ".txt");
                    PrintWriter out = new PrintWriter(file, "UTF-8");
                    out.println(firstPage);

                    // Upload to EC2, File name : workerID+index+".txt"
                    String newURL = myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, Header.OUTPUT_FOLDER_NAME, workerID+index+".txt", file);
                    outputLine = outputLine + newURL;
                    out.close();

                } else if (operation.equals("ToImage")){
                    // ToImage - convert the first page of the PDF file to a "png" image.
                    PDFImageWriter writer = new PDFImageWriter();
                    writer.writeImage(pddDocument, Header.IMAGE_FORMAT, null,1,1, Header.IMAGE_NAME);
                    // outputLine = outputLine + new java.io.File( "." ).getCanonicalPath() + File.separator + Header.IMAGE_NAME+index+"."+IMAGE_FORMAT;

                    // Upload to EC2
                    // PDFImageWrite write the png to temp1.png constantly
                    File file = new File(new java.io.File( "." ).getCanonicalPath() + File.separator + Header.IMAGE_NAME+1+"."+Header.IMAGE_FORMAT);
                    String newURL = myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, Header.OUTPUT_FOLDER_NAME, workerID+index+"."+Header.IMAGE_FORMAT, file);
                    outputLine = outputLine + newURL;

                }else if (operation.equals("ToHTML")){
                    // ToHTML - convert the first page of the PDF file to an HTML file.
                    PDFText2HTML pdfText2HTML = new PDFText2HTML(Header.ENCODING);
                    pdfText2HTML.setStartPage(1);
                    pdfText2HTML.setEndPage(1);
                    FileWriter fWriter = null;
                    BufferedWriter bufferedWriter = null;

                    // Create new file
                    fWriter = new FileWriter(Header.HTML_NAME + ".html");
                    bufferedWriter = new BufferedWriter(fWriter);
                    pdfText2HTML.writeText(pddDocument,bufferedWriter);
                    bufferedWriter.close();

                    // Upload to EC2
                    // outputLine = outputLine + new java.io.File( "." ).getCanonicalPath() + File.separator + HTML_NAME + ".html";
                    File file = new File(new java.io.File( "." ).getCanonicalPath() + File.separator + Header.HTML_NAME+".html");
                    String newURL = myAWS.mUploadS3(Header.APP_BUCKET_NAME+shortLocalAppID, Header.OUTPUT_FOLDER_NAME, workerID+index+".html", file);
                    outputLine = outputLine + newURL;
                    if (newURL == null){
                        // then something happen to the upload process
                        outputLine = null;
                    }

                }else{
                    outputLine = outputLine + "Error: Unsupported operation: " + operation;
                }
            }else{
                outputLine = outputLine + "Error: File is Encrypted";
            }
            pddDocument.close();

        } catch (AwsServiceException ase) {
            // if the problem is with AWS service return null so other worker will try to handle this request
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.statusCode());
            System.out.println("Error Type:       " + "AwsServiceException");
            System.out.println("AWS Error Code:   " + ase.awsErrorDetails().errorCode());
            System.out.println("Request ID:       " + ase.requestId());
            outputLine = null;

        } catch (AccountException ace) {
            // if the problem is with AWS service return null so other worker will try to handle this request
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
            outputLine = null;

        } catch (Exception e) {
            try{
                //outputLine = outputLine + "Error:" + e.getCause().getMessage();
                outputLine = outputLine + "Error: PDF file not found - " + e.getClass().getName();
            } catch (Exception ex){
                //outputLine = outputLine + "Error:" + e.getMessage();
                outputLine = outputLine + "Error: PDF file not found";
            }
        }
        return outputLine;
    }

    
    private static String analyzeMessage(LocalCloud myAWS, Message currMessage){
        String outputMessage = null;

        try {
            /*
             * msg = LocalAppID + "\t" + operation + " \t" + pdfURL
             * parsedMessage[0] = localAppID, first 12 is shortLocalAppID
             * parsedMessage[1] = operation
             * parsedMessage[2] = pdf URL
             */
            String[] parsedMessage = currMessage.body().split("\t");
            String shortLocalAppID = parsedMessage[0].substring(0, 12);
            String operation = parsedMessage[1];
            String fileURL = parsedMessage[2];

            outputMessage = convertPDF(myAWS, shortLocalAppID, operation, fileURL);

        }catch (Exception e){
            e.printStackTrace();
        }
        return outputMessage;
    }
    
    private static void initializeAllQueues(LocalCloud myAWS) {
        ArrayList<Map.Entry<String, String>> queues = new ArrayList<Map.Entry<String,String>>();

        // queue from Manager to Workers
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.INPUT_WORKERS_QUEUE_NAME, "0"));

        // queue from Workers to Manager
        queues.add(new AbstractMap.SimpleEntry<String, String>(Header.OUTPUT_WORKERS_QUEUE_NAME, "0"));

        myAWSsqsURL = myAWS.initSQSqueues(queues);
    }

}