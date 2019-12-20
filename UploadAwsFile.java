package csvfile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;

import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public class UploadAwsFile {

	public static void main(String[] args) {

		
		 Regions clientRegion = Regions.DEFAULT_REGION;
		String bucketName = "dataelements123";
		String KeyName = "gross-domestic-product-september-2019-quarter-csv.csv";
		String filepath = "s3.console.aws.amazon.com/s3/buckets/dataelements123/?region=ap-south-1&tab=overview";
		ClientConfiguration clientConfiguration = new ClientConfiguration();
		clientConfiguration.setConnectionTimeout(50000);
		clientConfiguration.setMaxConnections(500);
		clientConfiguration.setSocketTimeout(100000);
		clientConfiguration.setMaxErrorRetry(10);

		File file = new File("C:\\Users\\PC\\OneDrive\\Desktop\\filecsv\\business-operations-survey-2018-business-finance-csv.csv");
		long contentLength = file.length();
		long partSize = 2 * 1024 * 1024;
		

		try {
			AWSCredentials credentials = new BasicAWSCredentials("AKIAIRKDJ6MFDPE7DL","3z+JmfR5kWvw26/ymcHVzR7WpAoaYgstI6Y4G");

			 @SuppressWarnings("deprecation")
			AmazonS3 s3Client = new AmazonS3Client(credentials);

			 List<PartETag> partETags = new ArrayList<PartETag>();

			 
			InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest("dataelements123",
					"gross-domestic-product-september-2019-quarter-csv.csv");
			InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

			long filePosition = 0;
			for (int i = 1; filePosition < contentLength; i++) {
				
				partSize = Math.min(partSize, (contentLength - filePosition));

				UploadPartRequest uploadRequest = new UploadPartRequest()
						.withBucketName("dataelements123")
						.withKey("gross-domestic-product-september-2019-quarter-csv.csv")
						.withUploadId(initResponse.getUploadId())
						.withPartNumber(i)
						.withFileOffset(filePosition)
						.withFile(file)
						.withPartSize(partSize);

				UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
                partETags.add(uploadResult.getPartETag());

				filePosition += partSize;
				
				
			}
			CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest("dataelements123",
					"gross-domestic-product-september-2019-quarter-csv.csv",

					initResponse.getUploadId(), partETags);
			s3Client.completeMultipartUpload(compRequest);
			} catch (AmazonServiceException e) {
			
			e.printStackTrace();
		} catch (SdkClientException e) {
			
			e.printStackTrace();
		}
	
	}
}
