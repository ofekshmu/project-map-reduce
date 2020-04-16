
import java.io.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Random;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.AbstractMap.SimpleEntry;
import software.amazon.awssdk.services.sqs.SqsClient;
/**
 * Distributed System Programming : Cloud Computing and Map-Reducce1 - 2019/Spring
 * Assignment 1
 *
 * DSP Local Application
 * PDF Document Conversion in the Cloud
 *
 * Creators : Maor Assayag
 *            Refahel Shetrit
 *
 * LocalCLoud class - Barebone of AWS handling. Creating, Managing & Terminating instance of
 * S3 storage, EC2 instances and SQS queue of messages.
 */
public class LocalCloud {

    private SqsClient sqs;
    private Ec2Client ec2;
    private S3Client s3;
    //TODO:private  credentials;
    private boolean fromLocal;

    /**
     * LocalCLoud - get your credentials from the "credentials" file inside you .aws folder
     *
     * @param fromLocal doest the current java file is running locally or from the cloud
     */
    public LocalCloud(boolean fromLocal){
        this.fromLocal = fromLocal;
        if(fromLocal){
            //TODO: credentials = new ProfileCredentialsProvider().getCredentials();
        }
    }

    /**
     * initAWSservices - init all services
     */
    public void init_services(){
        initEC2();
        initS3();
        initSQS();
    }

    /**
     * initialize EC2 service, Region = US_EAST_1
     */
    public void initEC2(){ //ami-076515f20540e6e0b
       /** if (this.fromLocal){
            mEC2 = AmazonEC2ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }else{
            // We start instances on the cloud with IAM role
            mEC2 = AmazonEC2ClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }*/
    	
    	this.ec2 = Ec2Client.create();

    }

    /**
     * getEC2instanceID - find how many instances with tag & state is currently exists
     *
     * @param tag to identify the instance we are looking for
     * @param state of the instance (ex. running, stopped, pending etc)
     * @return id of instance found by Tag and state
     */
    public String getEC2instanceID(Tag tag, String state){
        List<Reservation> reservations = mEC2.describeInstances().getReservations();
        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                for (Tag instanceTag : instance.getTags()) {
                    if(instanceTag.getKey().equals(tag.getKey()) && instanceTag.getValue().equals(tag.getValue())){
                        // e.g.  the instance Tag name=Type and the value=Manager
                        if(instance.getState().getName().equals(state)) {
                            //System.out.println(instance.getInstanceId());
                            return instance.getInstanceId();
                        }
                    }
                }
            }
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
        List<Reservation> reservations = mEC2.describeInstances().getReservations();
        int ans = 0;
        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                for (Tag instanceTag : instance.getTags()) {
                    if(instanceTag.getKey().equals(tag.getKey()) && instanceTag.getValue().equals(tag.getValue())){
                        // e.g.  the instance Tag name=Type and the value=Manager
                        if(instance.getState().getName().equals(state)) {
                            //System.out.println(instance.getInstanceId());
                            ans++;
                        }
                    }
                }
            }
        }
        return ans;
    }

    public ArrayList<String> getEC2instancesByTagState(Tag tag, String state){
        List<Reservation> reservations = mEC2.describeInstances().getReservations();
        ArrayList<String> instancesId = new ArrayList<String>();
        for (Reservation reservation : reservations) {
            List<Instance> instances = reservation.getInstances();
            for (Instance instance : instances) {
                for (Tag instanceTag : instance.getTags()) {
                    if(instanceTag.getKey().equals(tag.getKey()) && instanceTag.getValue().equals(tag.getValue())){
                        // e.g.  the instance Tag name=Type and the value=Manager
                        if(instance.getState().getName().equals(state)) {
                            //System.out.println(instance.getInstanceId());
                            instancesId.add(instance.getInstanceId());
                        }
                    }
                }
            }
        }
        return instancesId;
    }
    /** 
     * initialize_manager - initialize a computer and sets tag as a manager 
     * @return the masters id
    */
    public String initialize_manager()
    {        
    	// define a request
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId("ami-076515f20540e6e0b") // recommanded image
                .instanceType(InstanceType.T1_MICRO)
                .maxCount(1)
                .minCount(1) // we want 1 computer for a manager
                .build();
        // a responce may take a set a mount of time before server returns all computers acording to request info
        RunInstancesResponse response = this.ec2.runInstances(runRequest); // response will be a list on instances
        
        String instanceId = response.instances().get(0).instanceId(); // we will take the first and only instance id
 
        Tag tag = Tag.builder() // set the tag
                .key("Name")
                .value("Manager")
                .build();
 		
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
 
        try {
            this.ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 instance %s based on AMI %s",
                    instanceId, "ami-076515f20540e6e0b");
        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        // snippet-end:[ec2.java2.create_instance.main]
        System.out.println("Manager has been initialized");
        return instanceId;
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
    public ArrayList<String> initEC2instance(String imageId, Integer minCount, Integer maxCount, String type, String bucketName, String userData, String keyName, Tag tag){
    	//TODO: ofeks note this works when a new manager needs to e assigned by the local or when a new worker needs to be assigned by manager
        ArrayList<String> instancesId = new ArrayList<String>();
        String userScript = null;

        try {
            userScript = getScript(bucketName, userData);

        } catch (Exception e) {
            //e.printStackTrace();
            System.out.println("\"             Starting an instance without a script \n");
        }

        // new request
        RunInstancesRequest request = new RunInstancesRequest(imageId, minCount, maxCount);
        request.setInstanceType(type);
        request.withKeyName(keyName);
        request.withIamInstanceProfile(new IamInstanceProfileSpecification().withName(keyName));
        if (userScript != null)
            request.withUserData(userScript);

        List<Instance> instances = mEC2.runInstances(request).getReservation().getInstances();
        List<Tag> tags = new ArrayList<Tag>();
        tags.add(tag);
        CreateTagsRequest tagsRequest = new CreateTagsRequest();
        tagsRequest.setTags(tags);
        String instanceID;

        // Create tag request for each instance (if we want to denote 20 workers then we want tags for them)
        for (Instance instance : instances) {
            try {
                if (instance.getState().getName().equals("pending") || instance.getState().getName().equals("running")){
                    instanceID = instance.getInstanceId();
                    tagsRequest.withResources(instanceID);
                    mEC2.createTags(tagsRequest);
                    instancesId.add(instanceID);
                }
            } catch (Exception e) {
                System.out.println("             Error Message on initEC2instance instances tag request : " + e.getMessage());
            }
        }
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
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(instanceId);
        mEC2.terminateInstances(request);
    }

    /** @param instanceId to terminate
     */
    public void terminateEC2instance(Collection<String> instanceId){
        TerminateInstancesRequest request = new TerminateInstancesRequest().withInstanceIds(instanceId);

        mEC2.terminateInstances(request);
    }

    public void terminateEC2all(){
        DescribeInstancesResult describeInstancesRequest = mEC2.describeInstances();
        List<Reservation> reservations = describeInstancesRequest.getReservations();

        Set<Instance> instances = new HashSet<Instance>();
        for (Reservation reservation : reservations) {
            instances.addAll(reservation.getInstances());
        }

        ArrayList<String> instancesId = new ArrayList<String>();
        for (Instance ins : instances){
            instancesId.add(ins.getInstanceId());
        }

        terminateEC2instance(instancesId);
    }

    /**
     * initialize S3 services
     */
    public void initS3(){
       /** if(this.fromLocal){
            mS3 = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }else{
            // We start instances on the cloud with IAM role
            mS3 = AmazonS3ClientBuilder
                    .standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }*/
    	Region region = Region.US_WEST_2;
        this.s3 = S3Client.builder().region(region).build();
        
        String bucket = "bucket" + System.currentTimeMillis();
        String key = "key";
 
        createBucket(bucket, region);
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
            mS3.createBucket(bucketName); // open connection with the S3 client
            mS3.putObject(new PutObjectRequest(bucketName, folderName + "/" + key, file)); // upload the file to the bucket
            return "https://s3.amazonaws.com/" + bucketName + "/" + folderName + "/" + key; // return the url of the uploaded file
        } else{
            mS3.createBucket(bucketName); // open connection with the S3 client
            mS3.putObject(new PutObjectRequest(bucketName, key, file)); // upload the file to the bucket
            return "https://s3.amazonaws.com/" + bucketName + "/" + key; // return the url of the uploaded file
        }
    }

    public void mCreateFolderS3(String bucketName, String folderName) {
        // create meta-data for your folder and set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        // create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);


        // create a PutObjectRequest passing the folder name suffixed by /
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                folderName + "/", emptyContent, metadata);

        // send request to S3 to create folder
        mS3.putObject(putObjectRequest);
    }

    /**
     * mDownloadS3file - if available download the desired file (by name key) from a bucket
     *
     * @param bucketName which bucket the file should be in
     * @param key "folder_name/file_name" : the file name + folder name if exists
     * @return the file object
     */
    public S3Object mDownloadS3file(String bucketName, String key){
        return mS3.getObject(new GetObjectRequest(bucketName, key));
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
        mS3.deleteObject(new DeleteObjectRequest(bucketName, key));
    }

    /**
     * mDeleteS3bucket - the bucket must be completely empty before it can be deleted
     * @param bucketName the bucket name to be deleted
     */
    public void mDeleteS3bucket(String bucketName) {
        mS3.deleteBucket(bucketName);
    }

    /**
     * Deletes a bucket and all the files inside
     */
    public void mDeleteS3bucketFiles(String bucketName){
        ObjectListing objectListing = mS3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            mDeleteS3file(bucketName, objectSummary.getKey());
        }
        mDeleteS3bucket(bucketName);
    }

    /**
     * initialize SQS services
     */
    public void initSQS(){
        /**if(this.fromLocal){
            mSQS = AmazonSQSClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }else{
            mSQS = AmazonSQSClientBuilder
                    .standard()
                    .withRegion(Regions.US_EAST_1)
                    .build();
        }*/
    	
    	this.sqs = SqsClient.builder().region(Region.US_WEST_2).build();
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
        mSQS.sendMessage(new SendMessageRequest(queueURL, message));
    }

    /**
     * receiveSQSmessage - receive messages from a specific queue
     *
     * @param queueURL URL of the queue we want to pull messages from
     * @return list of all the messages in the queue
     */
    public List<Message> receiveSQSmessage(String queueURL){
        // Create request to retrieve a list of messages in the SQS queue
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueURL);
        return mSQS.receiveMessage(request).getMessages();
    }

    /**
     * receiveSQSmessage - receive messages from a specific queue with request parameters
     *
     * @param request get messages with a personalized request
     * @return list of all messages received
     */
    public List<Message> receiveSQSmessage(ReceiveMessageRequest request){
        return mSQS.receiveMessage(request).getMessages();
    }

    /**
     * deleteSQSmessage - delete specific message from a queue
     *
     * @param queueUrl from which to delete
     * @param receiptHandle of the message to delete
     */
    public void deleteSQSmessage(String queueUrl, String receiptHandle) {
        mSQS.deleteMessage(new DeleteMessageRequest(queueUrl, receiptHandle));
    }

    /**
     * deleteSQSqueue - delete specific queue
     *
     * @param queueUrl URL of the queue
     */
    public void deleteSQSqueue(String queueUrl) {
        mSQS.deleteQueue(new DeleteQueueRequest(queueUrl));
    }

    /**
     * Delete all the Queues in SQS
     */
    public void deleteSQSqueueMessages(){
        for (String queueUrl : mSQS.listQueues().getQueueUrls()) {
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
}