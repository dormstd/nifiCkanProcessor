package net.atos.qrowd.processors.nifiCKANprocessor;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CKAN_API_Handler {
    // ToDo: Add log messages to the whole class
    private final Logger log = Logger.getLogger(CKAN_API_Handler.class);

    private String HOST;
    private String api_key;
    private String package_id;
    private String organization_id;
    private CloseableHttpClient httpclient;

    CKAN_API_Handler(String HOST, String api_key, String id_package, String organization_id) {
        this.HOST = HOST;
        this.api_key = api_key;
        this.package_id = id_package.toLowerCase();
        this.organization_id = organization_id.toLowerCase();
        this.httpclient = HttpClientBuilder.create().build();
    }

    // ToDo: Check if the package exists marked as delete, then reactivate it?
    public boolean packageExists() throws IOException{

        String line;
        StringBuilder sb = new StringBuilder();
        HttpPost postRequest;

        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("id",new StringBody(package_id,ContentType.TEXT_PLAIN))
                .build();

        postRequest = new HttpPost(HOST+"/api/action/package_show");
        postRequest.setEntity(reqEntity);
        postRequest.setHeader("X-CKAN-API-Key", api_key);

        HttpResponse response = httpclient.execute(postRequest);
        int statusCode = response.getStatusLine().getStatusCode();

        BufferedReader br = new BufferedReader(
                new InputStreamReader((response.getEntity().getContent())));
        while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append("\n");
        }

        if(statusCode==200)
        {
            log.info("Package with id "+package_id+" exists");
            //Check if that package is deleted

            log.info(sb);
            return true;
        }else{
            log.warn("Package with id "+package_id+" not found");
            log.warn(sb);
            return false;
        }
    }

    public void createPackage() throws IOException{

        HttpPost postRequest;
        StringBuilder sb = new StringBuilder();
        String line;

        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("name",new StringBody(package_id,ContentType.TEXT_PLAIN))
                .addPart("title",new StringBody(package_id,ContentType.TEXT_PLAIN))
                .addPart("owner_org",new StringBody(organization_id,ContentType.TEXT_PLAIN))
                //Package created private by default
                .addPart("private",new StringBody("true",ContentType.TEXT_PLAIN))
                .build();

        postRequest = new HttpPost(HOST+"/api/action/package_create");
        postRequest.setEntity(reqEntity);
        postRequest.setHeader("X-CKAN-API-Key", api_key);

        HttpResponse response = httpclient.execute(postRequest);
        int statusCode = response.getStatusLine().getStatusCode();

        BufferedReader br = new BufferedReader(
                new InputStreamReader((response.getEntity().getContent())));
        sb.append(statusCode);
        sb.append("\n");
        while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append("\n");
        }
        if(statusCode!=200){
            log.error("statusCode =!=" +statusCode);
            log.error(sb);
        }
        else {
            log.info("Request returns statusCode 200: OK");
            log.info(sb);
        }
    }

    public boolean organizationExists() throws IOException{
        String line;
        StringBuilder sb = new StringBuilder();
        HttpPost postRequest;

        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("id",new StringBody(organization_id,ContentType.TEXT_PLAIN))
                .build();

        postRequest = new HttpPost(HOST+"/api/action/organization_show");
        postRequest.setEntity(reqEntity);
        postRequest.setHeader("X-CKAN-API-Key", api_key);

        HttpResponse response = httpclient.execute(postRequest);
        int statusCode = response.getStatusLine().getStatusCode();

        BufferedReader br = new BufferedReader(
                new InputStreamReader((response.getEntity().getContent())));
        while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append("\n");
        }

        if(statusCode==200)
        {
            log.info("Organization with id "+organization_id+" exists");
            log.info(sb);
            return true;
        }else{
            log.warn("Organization with id "+organization_id+" not found");
            log.warn(sb);
            return false;
        }
    }

    public void createOrganization() throws IOException{

        HttpPost postRequest;
        StringBuilder sb = new StringBuilder();
        String line;

        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("name", new StringBody(organization_id, ContentType.TEXT_PLAIN))
                .addPart("id", new StringBody(organization_id, ContentType.TEXT_PLAIN))
                .addPart("title", new StringBody(organization_id, ContentType.TEXT_PLAIN))
                .build();

        postRequest = new HttpPost(HOST + "/api/action/organization_create");
        postRequest.setEntity(reqEntity);
        postRequest.setHeader("X-CKAN-API-Key", api_key);

        HttpResponse response = httpclient.execute(postRequest);
        int statusCode = response.getStatusLine().getStatusCode();

        BufferedReader br = new BufferedReader(
                new InputStreamReader((response.getEntity().getContent())));
        sb.append(statusCode);
        sb.append("\n");
        while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append("\n");
        }
        if (statusCode != 200) {
            log.error("statusCode =!=" + statusCode);
            log.error(sb);
        } else {
            log.info("Request returns statusCode 200: OK");
            log.info(sb);
        }
    }

    /**
     * Function that uploads a file to CKAN through it's API
     * @param path Local filesystem path of the file to upload
     * @return
     */
    public String uploadFile(String path) throws IOException {
        String line;
        StringBuilder sb = new StringBuilder();
        File file = new File(path);
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String date=dateFormatGmt.format(new Date());

        HttpPost postRequest;
        ContentBody cbFile = new FileBody(file, ContentType.TEXT_HTML);
        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("file", cbFile)
                .addPart("key", new StringBody(path+date,ContentType.TEXT_PLAIN))
                .addPart("url",new StringBody("testURL",ContentType.TEXT_PLAIN))
                .addPart("package_id",new StringBody(package_id,ContentType.TEXT_PLAIN))
                .addPart("upload",cbFile)
                .addPart("comment",new StringBody("comments",ContentType.TEXT_PLAIN))
                .addPart("notes", new StringBody("notes",ContentType.TEXT_PLAIN))
                .addPart("author",new StringBody("AuthorName",ContentType.TEXT_PLAIN))
                .addPart("author_email",new StringBody("AuthorEmail",ContentType.TEXT_PLAIN))
                .addPart("title",new StringBody("title",ContentType.TEXT_PLAIN))
                .addPart("description",new StringBody(path+date,ContentType.TEXT_PLAIN))
                .build();

        postRequest = new HttpPost(HOST+"/api/action/resource_create");
        postRequest.setEntity(reqEntity);
        postRequest.setHeader("X-CKAN-API-Key", api_key);

        HttpResponse response = httpclient.execute(postRequest);
        int statusCode = response.getStatusLine().getStatusCode();
        BufferedReader br = new BufferedReader(
                new InputStreamReader((response.getEntity().getContent())));

        sb.append(statusCode);
        sb.append("\n");
        if(statusCode!=200){
            log.error("statusCode =!=" +statusCode);
        }
        else log.info("Request returns statusCode 200: OK");

        while ((line = br.readLine()) != null) {
            sb.append(line);
            sb.append("\n");
            System.out.println("+"+line);
        }

        return sb.toString();
    }

    public void close()
    {
        httpclient.getConnectionManager().shutdown();
    }
}
