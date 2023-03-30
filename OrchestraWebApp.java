package com.alstom.function;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.sas.AccountSasPermission;
import com.azure.storage.common.sas.AccountSasResourceType;
import com.azure.storage.common.sas.AccountSasService;
import com.azure.storage.common.sas.AccountSasSignatureValues;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
/**
 * Azure Functions with HTTP Trigger.
 */
public class OrchestraWebApp {
	static String connectionString = System.getenv("ADLS_CONNECTION_String");
	static String TOKEN_TIMEOUT_IN_SECOND = System.getenv("TOKEN_TIMEOUT_IN_SECOND");

	//static String adlsBaseURL = System.getenv("ADLS_BASE_URL");
	//static int secretKey = 2;
	//static String containerName = System.getenv("ADLS_CONTAINER_NAME");
	/**
	 * This function listens at endpoint "/api/OrchestraWebApp". 
	 * Two ways to invoke it using "curl" command in bash:
	 * 1. curl -d "HTTP Body" {your host}/api/OrchestraWebApp
	 * 2. curl "{your host}/api/OrchestraWebApp?path=HTTP%20Query"
	 * Set following environment variables with correct values
	 * 1) ADLS_BASE_URL : Base url of ADLS container including container name 
	 * eg . https://sdc307553devsto01.blob.core.windows.net/orchestra-poc/
	 * 2) ADLS_CONNECTION_String : Get connection String from ADLS storage account and set it in environment variable
	 * 3) ADLS_CONTAINER_NAME : Get container name from ADLS storage account and set it in environment variable
	 * @throws URISyntaxException
	 * @throws InvalidKeyException
	 * @throws StorageException
	 */ 
	@FunctionName("OrchestraWebApp")
	public HttpResponseMessage run(
			@HttpTrigger(
					name = "req",
					methods = {HttpMethod.GET},
					authLevel = AuthorizationLevel.ANONYMOUS)
			HttpRequestMessage<Optional<String>> request,
			final ExecutionContext context) throws InvalidKeyException, URISyntaxException, StorageException {
		context.getLogger().info("OrchestraWebApp triggered.");

		// Parse query parameter
		final String query = request.getQueryParameters().get("path");
		final String path = request.getBody().orElse(query);

		//Convert and Display
		if (path == null) {
			context.getLogger().info("No path was passed on the query String or in the request body");
			return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass a path on the query String or in the request body").build();
		} else {
			//Separate filepath & filename from path Parameter  
			String filename = path.substring(path.lastIndexOf("/") + 1).trim();
			String filepath = path.substring(0, path.lastIndexOf("/")).trim();

			//Construct CloudBlob Oject for genrating BlobURI using ADLS Connection String
		    CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(connectionString);
            CloudBlobClient cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
            CloudBlobContainer Container = cloudBlobClient.getContainerReference(filepath);     
	        CloudBlob Blob = Container.getBlockBlobReference(filename);

			String BlobURI = Blob.getUri().toASCIIString();
			String sasToken = getSASToken(context);

            String finalURL = BlobURI+"?"+ sasToken;
			context.getLogger().info("finalURL: "+ finalURL);

			//Redirecting to Final URL
			return request.createResponseBuilder(HttpStatus.PERMANENT_REDIRECT)
					.header("Location", finalURL)
					.build();
		}
	}
	private String getSASToken(ExecutionContext context) {
	    String default_TokenTimeout = "300";
		
		//if No TOKEN_TIMEOUT_IN_SECOND defined in Env variable set default_TokenTimeout(300 sec) value to TOKEN_TIMEOUT_IN_SECOND 
		if (TOKEN_TIMEOUT_IN_SECOND == null || TOKEN_TIMEOUT_IN_SECOND.isEmpty()) {
			TOKEN_TIMEOUT_IN_SECOND = default_TokenTimeout; 
			context.getLogger().info("No TOKEN_TIMEOUT_IN_SECOND defined in Env variable, using default value as 300 secs");
		}try{
			//check if TOKEN_TIMEOUT_IN_SECOND contain any AlphaNumeric Value, if Alphanumeric use default_TokenTimeout in Catch Block
			Long ExpireTime = Long.parseLong(TOKEN_TIMEOUT_IN_SECOND);
		}
		catch(NumberFormatException x){
			TOKEN_TIMEOUT_IN_SECOND = default_TokenTimeout;
		context.getLogger().info("TOKEN_TIMEOUT_IN_SECOND is Not Long type (like alphanumeric) in env Variable, using default value as 300 secs.  NumberFormatException: "+x.getMessage());
		}
		BlobServiceClient blobClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();

		//Set the authority to be assigned to SAS.
		AccountSasPermission permission = AccountSasPermission.parse("r"); // read
		AccountSasService service = AccountSasService.parse("b"); //Specify the type of Storage service you want to use. This time Blob(b)is
		AccountSasResourceType resourceType = AccountSasResourceType.parse("oc"); //Select the resources you want to allow the operation. I want to operate Blob and container, so Object(o≒blob)And Container(c)To specify
		OffsetDateTime expireTime = OffsetDateTime.now().plusSeconds(Long.parseLong(TOKEN_TIMEOUT_IN_SECOND)); //Specifies the expiration date of the generated SAS token. Current time because it must be specified in OffsetDateTime+Let's allocate 10 minutes.
		
		DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
		context.getLogger().info("Time Now: "+ (OffsetDateTime.now()).format(dateFormatter));
		context.getLogger().info("Expire Time: "+ (OffsetDateTime.now().plusSeconds(Long.parseLong(TOKEN_TIMEOUT_IN_SECOND))).format(dateFormatter));
		
		//Account Sas that I worked hard to generate~Get AccountSasSignatureValues with arguments such as
		AccountSasSignatureValues sig = new AccountSasSignatureValues(expireTime, permission, service, resourceType);
		//SasIpRange sasIpRange = SasIpRange.parse("localhost");
		//sig.setSasIpRange(sasIpRange);
		//You can get SAS token.	
		return blobClient.generateAccountSas(sig);		
	}
}