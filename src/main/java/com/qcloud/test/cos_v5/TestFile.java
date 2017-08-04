package com.qcloud.test.cos_v5;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;


import org.testng.annotations.Test;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.Bucket;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.CannedAccessControlList;
import com.qcloud.cos.model.CopyObjectRequest;
import com.qcloud.cos.model.CopyObjectResult;
import com.qcloud.cos.model.CreateBucketRequest;
import com.qcloud.cos.model.DeleteBucketRequest;
import com.qcloud.cos.model.DeleteObjectRequest;
import com.qcloud.cos.model.GetObjectMetadataRequest;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ListObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PutObjectResult;
import com.qcloud.cos.model.UploadResult;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.Upload;

public class TestFile {
	
    String appid = "1251668577";
    String secret_id = "AKIDrbAYjEBqqdEconpFi8NPFsOjrnX4LYUE";
    String secret_key = "gCYjhT4ThiXAbp4aw65sTs56vY2Kcooc";
    String bucketName = "burningsouth";
    String localDirectory = "E:\\test\\testcos\\";
	
    /**
     * upload/download/head/delete 256K file object
     */
	@Test(invocationCount=6,threadPoolSize=1)
	public void test256KFile(){
		try {
	        COSCredentials cred = new BasicCOSCredentials(appid, secret_id, secret_key);
	        ClientConfig clientConfig = new ClientConfig(new Region("cn-south"));
	        COSClient cosClient = new COSClient(cred, clientConfig);
	        File directory = new File(localDirectory);
	        String prefix = "cos256Kfile";
	        String suffix = ".txt";
	        File localFile = createSampleFile(prefix,suffix,directory,256*1024);
	        String key = localFile.getName();
	        //upload object
	        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, localFile);
	        PutObjectResult putObjectResult = cosClient.putObject(putObjectRequest);
	        localFile.deleteOnExit();
	        //download object
	        File downFile = new File(localDirectory+localFile.getName()+"_download");
	        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
	        ObjectMetadata downObjectMeta = cosClient.getObject(getObjectRequest, downFile);
	        downFile.deleteOnExit();
	        if(putObjectResult.getETag().equals(downObjectMeta.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        // head object
	        GetObjectMetadataRequest getObjectMetadataRequest =
	                new GetObjectMetadataRequest(bucketName, key);
	        ObjectMetadata statObjectMeta = cosClient.getObjectMetadata(getObjectMetadataRequest);
	        if(statObjectMeta.getETag().equals(putObjectResult.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        //delete object
	        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, key);
	        cosClient.deleteObject(deleteObjectRequest);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	/**
     * upload/download/head/delete 4M file object
     */
	@Test(invocationCount=6,threadPoolSize=1)
	public void test4MFile(){
		try {
	        COSCredentials cred = new BasicCOSCredentials(appid, secret_id, secret_key);
	        ClientConfig clientConfig = new ClientConfig(new Region("cn-south"));
	        COSClient cosClient = new COSClient(cred, clientConfig);
	        File directory = new File(localDirectory);
	        String prefix = "cos4Mfile";
	        String suffix = ".txt";
	        File localFile = createSampleFile(prefix,suffix,directory,4*1024*1024);
	        String key = localFile.getName();
	        //upload object
	        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, localFile);
	        PutObjectResult putObjectResult = cosClient.putObject(putObjectRequest);
	        localFile.deleteOnExit();
	        //download object
	        File downFile = new File(localDirectory+localFile.getName()+"_download");
	        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
	        ObjectMetadata downObjectMeta = cosClient.getObject(getObjectRequest, downFile);
	        downFile.deleteOnExit();
	        if(putObjectResult.getETag().equals(downObjectMeta.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        // head object
	        GetObjectMetadataRequest getObjectMetadataRequest =
	                new GetObjectMetadataRequest(bucketName, key);
	        ObjectMetadata statObjectMeta = cosClient.getObjectMetadata(getObjectMetadataRequest);
	        if(statObjectMeta.getETag().equals(putObjectResult.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        //delete object
	        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, key);
	        cosClient.deleteObject(deleteObjectRequest);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	/**
     * upload/download/head/delete 100M file object
     */
	@Test(invocationCount=4,threadPoolSize=1)
	public void test100MFile(){
		try {
	        COSCredentials cred = new BasicCOSCredentials(appid, secret_id, secret_key);
	        ClientConfig clientConfig = new ClientConfig(new Region("cn-south"));
	        COSClient cosClient = new COSClient(cred, clientConfig);
	        File directory = new File(localDirectory);
	        String prefix = "cos100Mfile";
	        String suffix = ".txt";
	        File localFile = createSampleFile(prefix,suffix,directory,100*1024*1024);
	        String key = localFile.getName();
	        //upload object
	        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, localFile);
	        PutObjectResult putObjectResult = cosClient.putObject(putObjectRequest);
	        localFile.deleteOnExit();;
	        //download object
	        File downFile = new File(localDirectory+localFile.getName()+"_download");
	        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
	        ObjectMetadata downObjectMeta = cosClient.getObject(getObjectRequest, downFile);
	        downFile.deleteOnExit();
	        if(putObjectResult.getETag().equals(downObjectMeta.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        // head object
	        GetObjectMetadataRequest getObjectMetadataRequest =
	                new GetObjectMetadataRequest(bucketName, key);
	        ObjectMetadata statObjectMeta = cosClient.getObjectMetadata(getObjectMetadataRequest);
	        if(statObjectMeta.getETag().equals(putObjectResult.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        //delete object
	        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, key);
	        cosClient.deleteObject(deleteObjectRequest);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	/**
     * upload/download/head/delete 1G file object
     */
	@Test(invocationCount=1,threadPoolSize=1)
	public void test1GFile(){
		try {
	        COSCredentials cred = new BasicCOSCredentials(appid, secret_id, secret_key);
	        ClientConfig clientConfig = new ClientConfig(new Region("cn-south"));
	        COSClient cosClient = new COSClient(cred, clientConfig);
	        File directory = new File(localDirectory);
	        String prefix = "cos1Gfile";
	        String suffix = ".txt";
	        File localFile = createSampleFile(prefix,suffix,directory,1024*1024*1024);
	        String key = localFile.getName();
	        //upload object
	        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, localFile);
	        PutObjectResult putObjectResult = cosClient.putObject(putObjectRequest);
	        localFile.deleteOnExit();
	        //download object
	        File downFile = new File(localDirectory+localFile.getName()+"_download");
	        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
	        ObjectMetadata downObjectMeta = cosClient.getObject(getObjectRequest, downFile);
	        downFile.deleteOnExit();
	        if(putObjectResult.getETag().equals(downObjectMeta.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        // head object
	        GetObjectMetadataRequest getObjectMetadataRequest =
	                new GetObjectMetadataRequest(bucketName, key);
	        ObjectMetadata statObjectMeta = cosClient.getObjectMetadata(getObjectMetadataRequest);
	        if(statObjectMeta.getETag().equals(putObjectResult.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        //delete object
	        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, key);
	        cosClient.deleteObject(deleteObjectRequest);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	/**
     * multipart upload
     */
	@Test(invocationCount=1,threadPoolSize=1)
	public void testMultipartUploadFile(){
		try {
	        COSCredentials cred = new BasicCOSCredentials(appid, secret_id, secret_key);
	        ClientConfig clientConfig = new ClientConfig(new Region("cn-south"));
	        COSClient cosClient = new COSClient(cred, clientConfig);
	        File directory = new File(localDirectory);
	        String prefix = "multipartfile";
	        String suffix = ".txt";
	        File localFile = createSampleFile(prefix,suffix,directory,100*1024*1024);
	        String key = localFile.getName();
	        //multipart upload object
	        TransferManager transferManager = new TransferManager(cosClient);
	        Upload upload = transferManager.upload(bucketName, key, localFile);
	        // 等待传输结束
	        upload.waitForCompletion();
	        UploadResult uploadResult = upload.waitForUploadResult();
	        transferManager.shutdownNow();
//	        System.out.println(uploadResult.getETag());
	        localFile.deleteOnExit();
	        //download object
	        File downFile = new File(localDirectory+localFile.getName()+"_download");
	        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
	        ObjectMetadata downObjectMeta = cosClient.getObject(getObjectRequest, downFile);
//	        System.out.println(downObjectMeta.getETag());
	        downFile.deleteOnExit();
	        if(downObjectMeta.getETag().equals(uploadResult.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        // head object
	        GetObjectMetadataRequest getObjectMetadataRequest =
	                new GetObjectMetadataRequest(bucketName, key);
	        ObjectMetadata statObjectMeta = cosClient.getObjectMetadata(getObjectMetadataRequest);
	        if(statObjectMeta.getETag().equals(uploadResult.getETag())){
	        	assert true;
	        }else{
	        	assert false;
	        }
	        //delete object
	        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, key);
	        cosClient.deleteObject(deleteObjectRequest);
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
	
	/**
	 * list objects(get bucket)
	 */
	@Test(invocationCount=1,threadPoolSize=1)
	public void testListObjects(){
		try{
		   COSCredentials cred = new BasicCOSCredentials(appid, secret_id, secret_key);
           ClientConfig clientConfig = new ClientConfig(new Region("cn-south"));
           COSClient cosClient = new COSClient(cred, clientConfig);
        
           File directory = new File(localDirectory);
           String prefix = "testList";
           String suffix = ".txt";
           File localFile = createSampleFile(prefix,suffix,directory,1024*1024);
           String key = localFile.getName();
           //upload object
           PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, localFile);
           PutObjectResult putObjectResult = cosClient.putObject(putObjectRequest);
           localFile.deleteOnExit();
        
           ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(prefix);
		   ObjectListing objectListing = cosClient.listObjects(listObjectsRequest);
		   for(COSObjectSummary objectSummary : objectListing.getObjectSummaries()){
			   if(objectSummary.getKey().equals(key)){
				   assert objectSummary.getETag().equals(putObjectResult.getETag());
			   }
			   
		   };
		   //delete object
		   DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, key);
	       cosClient.deleteObject(deleteObjectRequest);
	       
		   cosClient.shutdown();
		}catch(Exception e) {
			// TODO: handle exception
		}
	}
	
	/**
	 * create and delete bucket
	 */
	@Test(invocationCount=1,threadPoolSize=1)
	public void testCreateAndDeleteBucket(){
		try{
			   COSCredentials cred = new BasicCOSCredentials(appid, secret_id, secret_key);
	           ClientConfig clientConfig = new ClientConfig(new Region("cn-south"));
	           COSClient cosClient = new COSClient(cred, clientConfig);
	           //create bucket
	           String bucketNameCreate = "bucketforcreate";
	           CreateBucketRequest createBucketRequest = new CreateBucketRequest(bucketNameCreate);
	           createBucketRequest.setCannedAcl(CannedAccessControlList.PublicReadWrite);
	           Bucket created_bucket = cosClient.createBucket(createBucketRequest);
	           System.out.println(created_bucket.getName());
	           
	           //delete bucket
	           DeleteBucketRequest deleteBucketRequest = new DeleteBucketRequest(bucketNameCreate);
	           cosClient.deleteBucket(deleteBucketRequest);
	           cosClient.shutdown();
			}catch(Exception e) {
				// TODO: handle exception
			}
	}
	
	/**
     * Creates a temporary file with text data to demonstrate uploading a file
     * to COS
     *
     * @return A newly created temporary file with text data.
     *
     * @throws IOException
     */
    private static File createSampleFile(String prefix, String suffix, File directory, long length) throws IOException {
        File file = File.createTempFile(prefix, suffix,directory);
        RandomAccessFile r = null;
        try {  
            r = new RandomAccessFile(file, "rw");  
            r.setLength(length);  
        } finally{  
            if (r != null) {  
                try {  
                    r.close();  
                } catch (IOException e) {  
                    e.printStackTrace();  
                }  
            }  
        }  
        return file;
    }

}
