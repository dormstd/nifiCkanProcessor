# Apache Nifi CKAN uploader

This is a custom [Apache Nifi](https://nifi.apache.org/) processor that, using the CKAN API, is able to:
* Check the existence and create organizations
* Check the existence and create packages
* Upload a file to a package

### Build and deploy

To deploy this processor in a Apache Nifi instance it first need to be packaged as a .nar file.
To do so just navigate into the project and run:
`mvn clean package`

Then we can deploy the generated .nar package that can be found in `~/nifi-nifiCKANprocessor-nar/target/`
into the libraries folder of the Apache Nifi instance.

### Configuration
The behaviour of this processor is very similar to the Nifi Fetch Files processor but adding some logic to handle the CKAN API.


It receives an input flowfile with a path to a file and handles the creation (if needed) of the organization and package in ckan to be able to upload that file to it.
After that logic finishes we can choose what to do with the processed file: Move it to a different folder, rename it, delete it...


The processor has 7 properties to be filled before running:

* CKAN_url: Url of the CKAN instance to write to
* file_path: Local path of the file to be uploaded to CKAN
* api_key: Personal API-Key provided by CKAN
* organization_id: Name of the organization to upload the file to, or create if it does not exists.
* COMPLETION_STRATEGY: What to do with the file after it is processed - Nothing, Move or Delete.
* MOVE_DESTINATION_DIR:(If COMPLETION_STRATEGY is set to Move) Path where the file will be moved to after processing
* CONFLICT_STRATEGY: (If COMPLETION_STRATEGY is set to Move) What to do if the destination file already exists - Rename, Replace, Keep or Fail.

