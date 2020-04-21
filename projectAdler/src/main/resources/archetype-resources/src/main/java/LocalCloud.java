import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import java.util.AbstractMap.SimpleEntry;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.ListQueuesRequest;
import software.amazon.awssdk.services.sqs.model.ListQueuesResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Distributed System Programming : Cloud Computing and Map-Reducce1 - 2020/Spring
 * Assignment 1
 */
public class LocalCloud {

    private SqsClient mSQS;
    private Ec2Client mEC2;
    private S3Client mS3;
    private InstanceProfileCredentialsProvider credentials;
    private boolean fromLocal;

    /**
     * LocalCloud - get your credentials from the "credentials" file inside you .aws folder
     * @param fromLocal doest the current java file is running locally or from the cloud
     */
    public LocalCloud(boolean fromLocal){
        this.fromLocal = fromLocal;
        if(fromLocal){
            credentials = InstanceProfileCredentialsProvider.builder()
            		.asyncCredentialUpdateEnabled(true)
            		.build();
        }
    }

    /**
     * initAWSservices - init all services
     */
    public void initAWSservices(){
        initEC2();
        initS3();
        initSQS();
    }

    /**
     * initialize EC2 service, Region = US_EAST_1
     */
    public void initEC2(){
        if (this.fromLocal){
        	this.mEC2 = Ec2Client.builder()
        			.credentialsProvider(this.credentials)  
        			.build();
        }else{
            // We start instances on the cloud with IAM role
        	this.mEC2 = Ec2Client.builder().build();
        }
    }

    /**
     * getEC2instanceID - find how many instances with tag & state is currently exists
     *
     * @param tag to identify the instance we are looking for
     * @param state of the instance (ex. running, stopped, pending etc)
     * @return id of instance found by Tag and state
     */
    public String getEC2instanceID(Tag tag, String state){
    	String nextToken = null;
    	
    	try {    
    		do {        
    			DescribeInstancesRequest request = DescribeInstancesRequest
    					.builder()
    					.nextToken(nextToken)
    					.build();        
    			DescribeInstancesResponse response = this.mEC2.describeInstances(request);
    			
    			for (Reservation reservation : response.reservations()) {            
    				for (Instance instance : reservation.instances()) {
    					//if(instance.getKey().equals(tag.getKey()) && instanceTag.getValue().equals(tag.getValue())){
    					for( Tag t : instance.tags())
                        	if(tag.equals(t) && instance.state().name().equals(state))
                        		return instance.instanceId();
    				}
    			}
    			nextToken = response.nextToken();
    			
    			} while (nextToken != null);
    		} catch (Ec2Exception e) {	
    			e.getStackTrace();
    		}
    	return null;
    }

    /**
     * getEC2instanceID - find how many instances with tag & state is currently exists
     *
     * @param tag to identify the instance we are looking for
     * @param state of the instance (ex. running, stopped, pending etc)
     * @return how many instances match this description
     */
    public int getNumEC2instancesByTagState(Tag tag, String state){
    	String nextToken = null;
    	int acc = 0;
    	try {    
    		do {        
    			DescribeInstancesRequest request = DescribeInstancesRequest
    					.builder()
    					.nextToken(nextToken)
    					.build();        
    			DescribeInstancesResponse response = this.mEC2.describeInstances(request);
    			
    			for (Reservation reservation : response.reservations()) {            
    				for (Instance instance : reservation.instances()) {
    					//if(instance.getKey().equals(tag.getKey()) && instanceTag.getValue().equals(tag.getValue())){
    					for( Tag t : instance.tags())
                        	if(tag.equals(t) && instance.state().name().equals(state))
                        		acc ++;
    				}
    			}
    			nextToken = response.nextToken();
    			
    			} while (nextToken != null);
    		} catch (Ec2Exception e) {	
    			e.getStackTrace();
    		}
    	return acc;
    }
    	


    public ArrayList<String> getEC2instancesByTagState(Tag tag, String state){
    	String nextToken = null;
        ArrayList<String> instancesId = new ArrayList<String>();
    	try {    
    		do {        
    			DescribeInstancesRequest request = DescribeInstancesRequest
    					.builder()
    					.nextToken(nextToken)
    					.build();        
    			DescribeInstancesResponse response = this.mEC2.describeInstances(request);
    			
    			for (Reservation reservation : response.reservations()) {            
    				for (Instance instance : reservation.instances()) {
    					//if(instance.getKey().equals(tag.getKey()) && instanceTag.getValue().equals(tag.getValue())){
    					for( Tag t : instance.tags())
                        	if(tag.equals(t) && instance.state().name().equals(state))
                        		instancesId.add(instance.instanceId());
    				}
    			}
    			nextToken = response.nextToken();
    			
    			} while (nextToken != null);
    		} catch (Ec2Exception e) {	
    			e.getStackTrace();
    		}
    	return instancesId;
    }

    /**
     * initEC2instance - init instances on EC2 AWS service
     *
     * @param imageId (for ex. "ami-b66ed3de")
     * @param minCount - min number of instances to be created, up to the system to decide if possible
     * @param maxCount - max number of instances to be created, the System will choose the min(possible, maxCount) to be created
     * @param type (ex. T2Small)
     * @param userData (txt file containing the script for the instance to run when started)
     * @param keyName (name for the new instance)
     * @param tag which tag attach to the new instances
     * @return list with all the instance's id created
     */
    public ArrayList<String> initEC2instance(String imageId, 
    		Integer minCount, 
    		Integer maxCount, 
    		String type, 
    		String bucketName, 
    		String userData, 
    		String keyName, 
    		Tag tag){

        ArrayList<String> instancesId = new ArrayList<String>();
        String userScript = null;

        try {
            userScript = getScript(bucketName, userData);

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("\"             Starting an instance without a script \n");
        }

        // new request
        
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(imageId)
                .instanceType(InstanceType.T1_MICRO)
                .maxCount(maxCount)
                .minCount(minCount)
                .build();

        RunInstancesResponse response = this.mEC2.runInstances(runRequest);

        //String instanceId = response.instances().get(0).instanceId();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .tags(tag)
                .build();
        //this.mEC2.createTags(tagRequest);
        // check if to create a new request each time, or edit the current one
        String instanceId;

        for (Instance instance : response.instances()) {
            try {
                if (instance.state().name().equals("pending") || instance.state().getName().equals("running")){
                    instanceId = instance.instanceId();
                    
                    tagRequest.ceId);
                            
                    
                    //tagsRequest.withResources(instanceID);
                    this.mEC2.createTags(tagsRequest);
                    instancesId.add(instanceId);
                }
            } catch (Ec2Exception e) {
                System.err.println(e.getMessage());
                System.exit(1);
            }
        }
        //-------------
        return instancesId;
    }

    /**
     * restartEC2instance
     * Used to restart already existing (but stopped) instances
     * @param instanceID - the stopped instance
     * @return if the stopped instance has been restart successfully
     */
    public Boolean restartEC2instance(String instanceID){
        try{
            StartInstancesRequest request = new StartInstancesRequest();
            request.withInstanceIds(instanceID);
            StartInstancesResult result = mEC2.startInstances(request);
            List<InstanceStateChange> instancesStates = result.getStartingInstances();
            for (InstanceStateChange instanceState : instancesStates){
                if (instanceState.getInstanceId().equals(instanceID)){
                    return instanceState.getCurrentState().getName().equals("running") || instanceState.getCurrentState().getName().equals("pending");
                }
            }
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    /**
     * @param instanceId to terminate
     */
    public void terminateEC2instance(String instanceId){
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
        		.instanceIds(instanceId)
        		.build();
        mEC2.terminateInstances(request);
    }

    /** @param instanceId to terminate
     */
    public void terminateEC2instance(Collection<String> instanceId){
    	TerminateInstancesRequest request = TerminateInstancesRequest.builder()
    			.instanceIds(instanceId)
    			.build();
        mEC2.terminateInstances(request);
    }

    public void terminateEC2all(){
        DescribeInstancesResponse describeInstancesRequest = mEC2.describeInstances();
        List<Reservation> reservations = describeInstancesRequest.reservations();

        Set<Instance> instances = new HashSet<Instance>();
        for (Reservation reservation : reservations) {
            instances.addAll(reservation.instances());
        }

        ArrayList<String> instancesId = new ArrayList<String>();
        for (Instance ins : instances){
            instancesId.add(ins.instanceId());
        }

        terminateEC2instance(instancesId);
    }

    /**
     * initialize S3 services
     */
    public void initS3(){
        if(this.fromLocal){
            this.mS3 = S3Client.builder()
            		.region(Region.US_WEST_2)        			
            		.credentialsProvider(this.credentials)  
            		.build();
        }else{
            // We start instances on the cloud with IAM role
        	this.mS3 = S3Client.builder()
            		.region(Region.US_WEST_2)        			
            		.build();
        }
        
        String bucket = "bucket" + System.currentTimeMillis();
        CreateBucketRequest createBucketRequest = CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(CreateBucketConfiguration.builder()		
                .locationConstraint(Region.US_WEST_2.id())
                .build())
                .build();
        this.mS3.createBucket(createBucketRequest);
    }

    /**
     * mUploadS3 - upload a file to specific bucket in S3
     *
     * @param bucketName the bucket name, needs to be valid
     * @param folderName the folder name in the bucket, can be null
     * @param key the desired name of the new file to be saved on S3 bucket
     * @param file the desired file to be uploaded
     * @return final S3 AWS url of the uploaded file
     */
    public String mUploadS3(String bucketName, String folderName, String key, File file){
    	
    	if (folderName != null){
            this.mS3.createBucket(bucketName); // open connection with the S3 client
            mS3.putObject(new PutObjectRequest(bucketName, folderName + "/" + key, file)); // upload the file to the bucket
            return "https://s3.amazonaws.com/" + bucketName + "/" + folderName + "/" + key; // return the url of the uploaded file
        } else{
            mS3.createBucket(bucketName); // open connection with the S3 client
            mS3.putObject(new PutObjectRequest(bucketName, key, file)); // upload the file to the bucket
            return "https://s3.amazonaws.com/" + bucketName + "/" + key; // return the url of the uploaded file
        }
    	/**
        if (folderName != null){
            mS3.createBucket(bucketName); // open connection with the S3 client
            mS3.putObject(new PutObjectRequest(bucketName, folderName + "/" + key, file)); // upload the file to the bucket
            return "https://s3.amazonaws.com/" + bucketName + "/" + folderName + "/" + key; // return the url of the uploaded file
        } else{
            mS3.createBucket(bucketName); // open connection with the S3 client
            mS3.putObject(new PutObjectRequest(bucketName, key, file)); // upload the file to the bucket
            return "https://s3.amazonaws.com/" + bucketName + "/" + key; // return the url of the uploaded file
        }
        */
    }

    public void mCreateFolderS3(String bucketName, String folderName) throws S3Exception, AwsServiceException, SdkClientException, IOException {
        // create meta-data for your folder and set content-length to 0
    	//S3ObjectMetadata metadata = S3ObjectMetadata.builder().build();
        //metadata.setContentLength(0);

        // create empty content
        //InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
        		.bucket(bucketName)
        		.build();


        // create a PutObjectRequest passing the folder name suffixed by /
        //PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
        //        folderName + "/", emptyContent, metadata);
        // send request to S3 to create folder
        this.mS3.putObject(putObjectRequest, RequestBody.fromByteBuffer(getRandomByteBuffer(0)));
    }

    /**	
     * mDownloadS3file - if available download the desired file (by name key) from a bucket
     *
     * @param bucketName which bucket the file should be in
     * @param key "folder_name/file_name" : the file name + folder name if exists
     * @return the file object
     */
    public S3Object mDownloadS3file(String bucketName, String key){
    	this.mS3.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build());
    	//return this.mS3.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build());
    }

    /**
     * doesFileExist - check if a file exists in a bucket by the file name
     * @param bucketName which bucket the file should be in
     * @param key "folder_name/file_name" : the file name + folder name if exists
     * @return true if the file exists in this bucket
     */
    public boolean doesFileExist(String bucketName, String key){
        return mS3.doesObjectExist(bucketName, key);
    }

    /**
     * mDeleteS3file - Delete file from a bucket
     *
     * @param bucketName which bucket the file should be in
     * @param key "folder_name/file_name" : the file name + folder name if exists
     */
    public void mDeleteS3file(String bucketName, String key){
    	DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketName).key(key).build();
        this.mS3.deleteObject(deleteObjectRequest);
    }

    /**
     * mDeleteS3bucket - the bucket must be completely empty before it can be deleted
     * @param bucketName the bucket name to be deleted
     */
    public void mDeleteS3bucket(String bucketName) {
    	DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder()
    			.bucket(bucketName)
    			.build();
        this.mS3.deleteBucket(deleteBucketRequest);
    }

    /**
     * Deletes a bucket and all the files inside
     */
    public void mDeleteS3bucketFiles(String bucketName){

        try {
             ListObjectsRequest listObjects = ListObjectsRequest
                     .builder()
                     .bucket(bucketName)
                     .build();

             ListObjectsResponse res = this.mS3.listObjects(listObjects);
             List<S3Object> objects = res.contents();
             
             for (S3Object objectSummary : objects) {
                 mDeleteS3file(bucketName, objectSummary.key());
             }
             mDeleteS3bucket(bucketName);
             
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    	
    }

    /**
     * initialize SQS services
     */
    public void initSQS(){
        if(this.fromLocal){
        	this.mSQS = SqsClient.builder()
                    .region(Region.US_WEST_2)
        			.credentialsProvider(this.credentials)  
                    .build();
        }else{
        	this.mSQS = SqsClient.builder()
                    .region(Region.US_WEST_2)
                    .build();
        }
    	String queueName = "queue" + System.currentTimeMillis();
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
        		.queueName(queueName)
        		.build();
    	this.mSQS.createQueue(createQueueRequest);
    }

    /**
     * initSQSqueues - Initialize a list of queues
     *
     * @param queues list of queues' names and their visibility timeout to be initialized
     * @return list of each queue's URL
     */
    public HashMap<String, String> initSQSqueues(ArrayList<Entry<String, String>> queues){
        HashMap<String, String> queuesURLs = new HashMap<String, String>();
        String queueURL = null;

        for (Entry<String, String> pair : queues) {
            String queueName = pair.getKey();
            try {
                queueURL = mSQS.getQueueUrl(queueName).getQueueUrl();
            }
            catch(AmazonServiceException exception) {
                if (exception.getStatusCode() == 400) { // not found
                    CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
                    Map<String, String> attributes = new HashMap<String, String>();
                    attributes.put("VisibilityTimeout", pair.getValue());
                    createQueueRequest.setAttributes(attributes);
                    queueURL = mSQS.createQueue(createQueueRequest).getQueueUrl();
                    System.out.println("             The following queue has been created : " + queueURL + "\n");
                }
                else {
                    System.out.println("Caught Exception: " + exception.getMessage());
                    System.out.println("Reponse Status Code: " + exception.getStatusCode());
                    System.out.println("Error Code: " + exception.getErrorCode());
                    System.out.println("Request ID: " + exception.getRequestId());
                }
            } catch (Exception exception){
                exception.printStackTrace();
            }
            queuesURLs.put(queueName, queueURL);
        }
        return queuesURLs;
    }

    /**
     * initSQSqueues - initialize only 1 queue
     *
     * @param queueName to initialize
     * @param visibilityTimeout default visibility time-out of messages in this queue
     * @return URL of the queue
     */
    public String initSQSqueues(String queueName, String visibilityTimeout){
        ArrayList<Entry<String, String>> queue = new ArrayList<Entry<String, String>>();
        queue.add(new SimpleEntry<String, String>(queueName, visibilityTimeout));
        return initSQSqueues(queue).get(queueName);
    }

    /**
     * sendSQSmessage - send a messages to a specific queue
     *
     * @param queueURL URL of the queue to which we want to send the message
     * @param message the message to be sent
     */
    public void sendSQSmessage(String queueURL, String message){
    	this.mSQS.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueURL)
                .messageBody(message)
                .build());
    	}

    /**
     * receiveSQSmessage - receive messages from a specific queue
     *
     * @param queueURL URL of the queue we want to pull messages from
     * @return list of all the messages in the queue
     */
    public List<Message> receiveSQSmessage(String queueURL){
        // Create request to retrieve a list of messages in the SQS queue
    	 ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                 .queueUrl(queueURL).build();
        return this.mSQS.receiveMessage(request).messages();
    }

    /**
     * receiveSQSmessage - receive messages from a specific queue with request parameters
     *
     * @param request get messages with a personalized request
     * @return list of all messages received
     */
    public List<Message> receiveSQSmessage(ReceiveMessageRequest request){
        return this.mSQS.receiveMessage(request).messages();
    }

    /**
     * deleteSQSmessage - delete specific message from a queue
     *
     * @param queueUrl from which to delete
     * @param receiptHandle of the message to delete
     */
    public void deleteSQSmessage(String queueUrl, String receiptHandle) {
    	DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();
        this.mSQS.deleteMessage(deleteMessageRequest);
    }

    /**
     * deleteSQSqueue - delete specific queue
     *
     * @param queueUrl URL of the queue
     */
    public void deleteSQSqueue(String queueUrl) {
    	DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
    			.queueUrl(queueUrl)
    			.build();
        this.mSQS.deleteQueue(deleteQueueRequest);
    }

    /**
     * Delete all the Queues in SQS
     */
    public void deleteSQSqueueMessages(){
        for (String queueUrl : this.mSQS.listQueues().queueUrls()) {
            deleteSQSqueue(queueUrl);
        }
    }

    /**
     * getScript - download a script file from S3, parsed it to Base64 to be attached to
     * userData of new instances (boot-strapping).
     *
     * @param userData file containing the script
     * @return script encoded in base64
     */
    private String getScript(String bucketName, String userData) {
        //Download script from S3
        S3Object object = mDownloadS3file(bucketName, userData);
        InputStream input = object.getObjectContent();

        String script = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        try {
            StringBuilder stringBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null){
                stringBuilder.append(line);
                stringBuilder.append("\n");
            }
            script = stringBuilder.toString();
            reader.close();
        } catch (Exception exception){
            exception.printStackTrace();
        }

        String ans = null;
        try{
            ans = new String(Base64.encode(script.getBytes()));
        }catch (NullPointerException npe){
            npe.printStackTrace();
        }
        return ans;
    }
    private static ByteBuffer getRandomByteBuffer(int size) throws IOException {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return ByteBuffer.wrap(b);
    }
}