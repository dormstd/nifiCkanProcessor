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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.IOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

@Tags({"ckan","web service","request","files","local"})
@CapabilityDescription("Nifi Processor that will upload the specified file to CKAN through its API, it will create the organization and package if needed.")
public class CKAN_File_Uploader extends AbstractProcessor {

    private static final AllowableValue COMPLETION_NONE = new AllowableValue("None", "None", "Leave the file as-is");
    private static final AllowableValue COMPLETION_MOVE = new AllowableValue("Move File", "Move File", "Moves the file to the directory specified by the <Move Destination Directory> property");
    private static final AllowableValue COMPLETION_DELETE = new AllowableValue("Delete File", "Delete File", "Deletes the original file from the file system");

    private static final AllowableValue CONFLICT_REPLACE = new AllowableValue("Replace File", "Replace File", "The newly ingested file should replace the existing file in the Destination Directory");
    private static final AllowableValue CONFLICT_KEEP_INTACT = new AllowableValue("Keep Existing", "Keep Existing", "The existing file should in the Destination Directory should stay intact and the newly "
            + "ingested file should be deleted");
    private static final AllowableValue CONFLICT_FAIL = new AllowableValue("Fail", "Fail", "The existing destination file should remain intact and the incoming FlowFile should be routed to failure");
    private static final AllowableValue CONFLICT_RENAME = new AllowableValue("Rename", "Rename", "The existing destination file should remain intact. The newly ingested file should be moved to the "
            + "destination directory but be renamed to a random filename");


    private static final PropertyDescriptor CKAN_url = new PropertyDescriptor
            .Builder().name("CKAN_url")
            .displayName("CKAN Url")
            .description("Hostname of the CKAN instance to write to")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .required(true)
            .build();
    private static final PropertyDescriptor file_path = new PropertyDescriptor
            .Builder().name("file_path")
            .displayName("File Path")
            .description("Absolute route to the file to be uploaded to CKAN")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .defaultValue("${absolute.path}/${filename}")
            .required(true)
            .build();
    private static final PropertyDescriptor api_key = new PropertyDescriptor
            .Builder().name("Api_Key")
            .displayName("File Api_Key")
            .description("Api Key to be used to interact with CKAN")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();
    private static final PropertyDescriptor organization_id = new PropertyDescriptor
            .Builder().name("organization_id")
            .displayName("Organization id to add the file to")
            .description("Organization id to add the package to, or create if necessary. Must contain only alphanumeric characters.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .required(true)
            .build();
    private static final PropertyDescriptor package_name = new PropertyDescriptor
            .Builder().name("package_name")
            .displayName("Name of the package to add the file to")
            .description("Name of the package to add the package to, or create if necessary. Must contain only alphanumeric characters. In case this is empty, the name of the file will be used.")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .required(true)
            .build();
    private static final PropertyDescriptor COMPLETION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Completion Strategy")
            .description("Specifies what to do with the original file on the file system once it has been pulled into NiFi")
            .expressionLanguageSupported(false)
            .allowableValues(COMPLETION_NONE, COMPLETION_MOVE, COMPLETION_DELETE)
            .defaultValue(COMPLETION_NONE.getValue())
            .required(true)
            .build();
    private static final PropertyDescriptor MOVE_DESTINATION_DIR = new PropertyDescriptor.Builder()
            .name("Move Destination Directory")
            .description("The directory to the move the original file to once it has been fetched from the file system. This property is ignored unless the Completion Strategy is set to \"Move File\". "
                    + "If the directory does not exist, it will be created.")
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .build();
    private static final PropertyDescriptor CONFLICT_STRATEGY = new PropertyDescriptor.Builder()
            .name("Move Conflict Strategy")
            .description("If Completion Strategy is set to Move File and a file already exists in the destination directory with the same name, this property specifies "
                    + "how that naming conflict should be resolved")
            .allowableValues(CONFLICT_RENAME, CONFLICT_REPLACE, CONFLICT_KEEP_INTACT, CONFLICT_FAIL)
            .defaultValue(CONFLICT_RENAME.getValue())
            .required(true)
            .build();

    private static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();
    private static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("Any FlowFile that could not be fetched from the file system because the file could not be found will be transferred to this Relationship.")
            .build();
    private static final Relationship REL_PERMISSION_DENIED = new Relationship.Builder()
            .name("permission.denied")
            .description("Any FlowFile that could not be fetched from the file system due to the user running NiFi not having sufficient permissions will be transferred to this Relationship.")
            .build();
    private static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "Any FlowFile that could not be fetched from the file system for any reason other than insufficient permissions or the file not existing will be transferred to this Relationship.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(CKAN_url);
        descriptors.add(file_path);
        descriptors.add(api_key);
        descriptors.add(organization_id);
        descriptors.add(COMPLETION_STRATEGY);
        descriptors.add(MOVE_DESTINATION_DIR);
        descriptors.add(CONFLICT_STRATEGY);
        descriptors.add(package_name);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_NOT_FOUND);
        relationships.add(REL_PERMISSION_DENIED);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        //This is the way to get the value of a property
        String url = context.getProperty(CKAN_url).getValue();
        final String filepath = context.getProperty(file_path).evaluateAttributeExpressions(flowFile).getValue();
        final File file = new File(filepath);
        final String apiKey = context.getProperty(api_key).getValue();

        //If the property package_name is not filled, then use the filename as package name
        String packagename = context.getProperty(package_name).getValue();
        if(packagename.isEmpty())
        {
            packagename=getFileName(file);
        }
        final String organizationId = context.getProperty(organization_id).getValue();

        if (flowFile == null) {
            return;
        }

        // Verify that file system is reachable and file exists
        Path filePath = file.toPath();
        if (!Files.exists(filePath) && !Files.notExists(filePath)) { // see https://docs.oracle.com/javase/tutorial/essential/io/check.html for more details
            getLogger().log(LogLevel.ERROR, "Could not fetch file {} from file system for {} because the existence of the file cannot be verified; routing to failure",
                    new Object[]{file, flowFile});
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        } else if (!Files.exists(filePath)) {
            getLogger().log(LogLevel.ERROR, "Could not fetch file {} from file system for {} because the file does not exist; routing to not.found", new Object[]{file, flowFile});
            session.getProvenanceReporter().route(flowFile, REL_NOT_FOUND);
            session.transfer(session.penalize(flowFile), REL_NOT_FOUND);
            return;
        }

        // Verify read permission on file
        final String user = System.getProperty("user.name");
        if (!isReadable(file)) {
            getLogger().log(LogLevel.ERROR, "Could not fetch file {} from file system for {} due to user {} not having sufficient permissions to read the file; routing to permission.denied",
                    new Object[]{file, flowFile, user});
            session.getProvenanceReporter().route(flowFile, REL_PERMISSION_DENIED);
            session.transfer(session.penalize(flowFile), REL_PERMISSION_DENIED);
            return;
        }
        // If configured to move the file and fail if unable to do so, check that the existing file does not exist and that we have write permissions
        // for the parent file.
        final String completionStrategy = context.getProperty(COMPLETION_STRATEGY).getValue();
        final String targetDirectoryName = context.getProperty(MOVE_DESTINATION_DIR).evaluateAttributeExpressions(flowFile).getValue();
        if (targetDirectoryName != null) {
            final File targetDir = new File(targetDirectoryName);
            if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
                if (targetDir.exists() && (!isWritable(targetDir) || !isDirectory(targetDir))) {
                    getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                                    + "but that is not a directory or user {} does not have permissions to write to that directory",
                            new Object[] {file, flowFile, targetDir, user});
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }

                final String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();

                if (CONFLICT_FAIL.getValue().equalsIgnoreCase(conflictStrategy)) {
                    final File targetFile = new File(targetDir, file.getName());
                    if (targetFile.exists()) {
                        getLogger().error("Could not fetch file {} from file system for {} because Completion Strategy is configured to move the original file to {}, "
                                        + "but a file with name {} already exists in that directory and the Move Conflict Strategy is configured for failure",
                                new Object[] {file, flowFile, targetDir, file.getName()});
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }
                }
            }
        }


        //  *******************
        //   Main logic of the CKAN uploader
        // - Create the CKAN API Handler
        // - Check that the targete organization exists in CKAN
        //      - If it doesn't, create it
        // - Check if the package exists in CKAN
        //      - If it doesn't, create it
        // - Upload the file to CKAN, with it's filename as ID
        // -- In case of any exception in the process, send the flowfile to FAILURE.
        // *********************

        CKAN_API_Handler ckan_api_handler = new CKAN_API_Handler(url, apiKey, packagename, organizationId);
        try {
            if (!ckan_api_handler.organizationExists()) {
                ckan_api_handler.createOrganization();
            }
            if (!ckan_api_handler.packageExists()) {
                ckan_api_handler.createPackage();
            }
            ckan_api_handler.uploadFile(file.getAbsolutePath());
            session.transfer(flowFile, REL_SUCCESS);
            ckan_api_handler.close();
        }catch(IOException ioe)
        {
            getLogger().log(LogLevel.ERROR, "Error while uploading file {} to CKAN {}: Organization {}.",
                    new Object[]{file.getName(), url, organizationId });
            session.transfer(session.penalize(flowFile), REL_FAILURE);
        }



        // It is critical that we commit the session before we perform the Delete. Otherwise, we could have a case where we
        // ingest the file, delete it, and then NiFi is restarted before the session is committed. That would result in data loss.
        // As long as we commit the session right here, we are safe.
        session.commit();

        // Attempt to perform the Completion Strategy action
        Exception completionFailureException = null;
        if (COMPLETION_DELETE.getValue().equalsIgnoreCase(completionStrategy)) {
            // convert to path and use Files.delete instead of file.delete so that if we fail, we know why
            try {
                delete(file);
            } catch (final IOException ioe) {
                completionFailureException = ioe;
            }
        } else if (COMPLETION_MOVE.getValue().equalsIgnoreCase(completionStrategy)) {
            final File targetDirectory = new File(targetDirectoryName);
            final File targetFile = new File(targetDirectory, file.getName());
            try {
                if (targetFile.exists()) {
                    final String conflictStrategy = context.getProperty(CONFLICT_STRATEGY).getValue();
                    if (CONFLICT_KEEP_INTACT.getValue().equalsIgnoreCase(conflictStrategy)) {
                        // don't move, just delete the original
                        Files.delete(file.toPath());
                    } else if (CONFLICT_RENAME.getValue().equalsIgnoreCase(conflictStrategy)) {
                        // rename to add a random UUID but keep the file extension if it has one.
                        final String simpleFilename = targetFile.getName();
                        final String newName;
                        if (simpleFilename.contains(".")) {
                            newName = StringUtils.substringBeforeLast(simpleFilename, ".") + "-" + UUID.randomUUID().toString() + "." + StringUtils.substringAfterLast(simpleFilename, ".");
                        } else {
                            newName = simpleFilename + "-" + UUID.randomUUID().toString();
                        }

                        move(file, new File(targetDirectory, newName), false);
                    } else if (CONFLICT_REPLACE.getValue().equalsIgnoreCase(conflictStrategy)) {
                        move(file, targetFile, true);
                    }
                } else {
                    move(file, targetFile, false);
                }
            } catch (final IOException ioe) {
                completionFailureException = ioe;
            }
        }

        // Handle completion failures
        if (completionFailureException != null) {
            getLogger().warn("Successfully fetched the content from {} for {} but failed to perform Completion Action due to {}; routing to success",
                    new Object[] {file, flowFile, completionFailureException}, completionFailureException);
        }
    }

    // Auxiliary methods got from fetch files processor
    private void move(final File source, final File target, final boolean overwrite) throws IOException {
        final File targetDirectory = target.getParentFile();

        // convert to path and use Files.move instead of file.renameTo so that if we fail, we know why
        final Path targetPath = target.toPath();
        if (!targetDirectory.exists()) {
            Files.createDirectories(targetDirectory.toPath());
        }

        final CopyOption[] copyOptions = overwrite ? new CopyOption[] {StandardCopyOption.REPLACE_EXISTING} : new CopyOption[] {};
        Files.move(source.toPath(), targetPath, copyOptions);
    }
    private void delete ( final File file) throws IOException {
        Files.delete(file.toPath());
    }
    private boolean isReadable(final File file) {
        return file.canRead();
    }

    private String getFileName(File file){

        getLogger().log(LogLevel.ERROR,"Filename to be processed: " + file.getName());
        return file.getName().split("\\.")[0];
    }

    private boolean isWritable(final File file) {
        return file.canWrite();
    }

    private boolean isDirectory(final File file) {
        return file.isDirectory();
    }

}
