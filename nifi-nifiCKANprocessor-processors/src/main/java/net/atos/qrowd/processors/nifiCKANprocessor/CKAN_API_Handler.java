/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.atos.qrowd.processors.nifiCKANprocessor;

import com.google.gson.Gson;
import net.atos.qrowd.processors.pojos.ResourceResponse;
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
import java.util.ResourceBundle;

public class CKAN_API_Handler {
    private final Logger log = Logger.getLogger(CKAN_API_Handler.class);

    private String HOST;
    private String api_key;
    private String package_id;
    private String organization_id;
    private String package_description;
    private CloseableHttpClient httpclient;
    private Boolean package_private;

    CKAN_API_Handler(String HOST, String api_key, String filename, String organization_id, String package_description, Boolean package_private) {
        this.HOST = HOST;
        this.api_key = api_key;
        this.package_id = filename.toLowerCase();
        this.package_description = package_description;
        this.organization_id = organization_id.toLowerCase();
        this.package_private = package_private;

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
                .addPart("owner_org",new StringBody(organization_id,ContentType.TEXT_PLAIN))
                .addPart("notes",new StringBody(package_description,ContentType.TEXT_PLAIN))
                .addPart("private",new StringBody(package_private.toString(),ContentType.TEXT_PLAIN))
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
        //ToDo: Save the returned package to store it's alfanumerical id (to be later used when updating the file)
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
                .addPart("key", new StringBody(file.getName().split("\\.")[0],ContentType.TEXT_PLAIN))
                .addPart("name", new StringBody(file.getName(),ContentType.TEXT_PLAIN))
                .addPart("url",new StringBody("testURL",ContentType.TEXT_PLAIN))
                .addPart("package_id",new StringBody(package_id,ContentType.TEXT_PLAIN))
                .addPart("upload",cbFile)
                .addPart("description",new StringBody(file.getName()+" created on: "+date,ContentType.TEXT_PLAIN))
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

    public String updateFile(String path, String resourceId) throws IOException {
        String line;
        StringBuilder sb = new StringBuilder();
        File file = new File(path);
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String date=dateFormatGmt.format(new Date());

        HttpPost postRequest;
        ContentBody cbFile = new FileBody(file, ContentType.TEXT_HTML);
        HttpEntity reqEntity = MultipartEntityBuilder.create()
                .addPart("id",new StringBody(resourceId,ContentType.TEXT_PLAIN))
                .addPart("file", cbFile)
                .addPart("key", new StringBody(file.getName().split("\\.")[0],ContentType.TEXT_PLAIN))
                .addPart("name", new StringBody(file.getName(),ContentType.TEXT_PLAIN))
                .addPart("url",new StringBody("testURL",ContentType.TEXT_PLAIN))
                .addPart("package_id",new StringBody(package_id,ContentType.TEXT_PLAIN))
                .addPart("upload",cbFile)
                .addPart("description",new StringBody(file.getName()+" created on: "+date,ContentType.TEXT_PLAIN))
                .build();

        postRequest = new HttpPost(HOST+"/api/action/resource_update");
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

    public Boolean createOrUpdateResource(String path) throws IOException {
        File file = new File(path);
        String filename = file.getName();
        HttpPost postRequest;
        StringBuilder sb = new StringBuilder();
        String line;

        Gson gson = new Gson();

        //query the API to get the resources with that file name
        postRequest = new HttpPost(HOST+"/api/action/resource_search?query=name:"+filename);
        postRequest.setHeader("X-CKAN-API-Key", api_key);

        HttpResponse response = httpclient.execute(postRequest);
        BufferedReader br = new BufferedReader(
                new InputStreamReader((response.getEntity().getContent())));
        while ((line = br.readLine()) != null) {
            sb.append(line);
        }
        //Parse the response into a POJO to be able to get results from it.
        ResourceResponse resResponse = gson.fromJson(sb.toString(),ResourceResponse.class);
        System.out.println(resResponse);
        //Now we need to check if the count of results is 1 (otherwise error)
        //if the count is 0, call uploadFile to create the file
        if(resResponse.getResult().getCount()==0)
        {
            log.info("No resource found under that name, creating it...");
            uploadFile(path);
            return true;
        //if the count is 1, get all the needed data to update the resource
        }else if(resResponse.getResult().getCount()==1)
        {
            String id = resResponse.getResult().getResults().get(0).getId();
            String result_package_id = resResponse.getResult().getResults().get(0).getPackageId();
            //ToDo: Check if the resource belongs to the same package
            //result_package_id is the id, package_id is the name of the package: How to get the alfanumeric ID?
            //if(result_package_id.equals(package_id)) {
                log.info("Resource found, updating it");
                updateFile(path, id);
                return true;
            /**}else{
                log.error("The found resource does not belong to the same package we are expecting");
                log.error("Package id found:"+result_package_id+". Package expected:"+package_id);
                return false;
            }*/
        }else{
            log.error("Found more than one resource with that name. Cancel update...");
            return false;
        }
    }

    public void close()
    {
        httpclient.getConnectionManager().shutdown();
    }
}
