# <img width="40" alt="image" src="https://private-user-images.githubusercontent.com/83917694/289161894-60f8f158-3bdc-4b3d-94ae-27a12441e2a3.png"> GCP Data Engineering Project: Building and Orchestrating an ETL Pipeline for Online Food Delivery Industry with Apache Beam and Apache Airflow

This GCP Data Engineering project focuses on developing a robust ETL (Extract, Transform, Load) pipeline for the online food delivery industry. The pipeline is designed to handle batch transactional data and leverages various Google Cloud Platform (GCP) services:

- <img width="18" alt="image" src="https://github.com/evanwillistech/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/6b5ffddf-0f74-41cf-8303-22f675cabdda"> GCS is used to store and manage the transactional data
- <img width="18" alt="image" src="https://github.com/evanwillistech/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/4c57cf42-15d3-4ba3-bad6-65b7fb9c5094"> Composer, a managed Apache Airflow service, is utilized to orchestrate Dataflow jobs
- <img width="18" alt="image" src="https://github.com/evanwillistech/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/69ceceb2-73a4-4b8b-9eb1-1207e7b5c5e3"> Dataflow, based on Apache Beam, is responsible for data processing, transformation, and loading into BigQuery
- <img width="18" alt="image" src="https://github.com/evanwillistech/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/48de05df-2b34-4d82-b2c5-e81809e9322c"> BigQuery serves as a serverless data warehouse
- <img width="18" alt="image" src="https://seeklogo.com/images/G/google-looker-logo-B27BD25E4E-seeklogo.com.png"> Looker, a business intelligence and analytics platform, is employed to generate daily reports

These technologies work together to efficiently process, store, and generate reports on the daily transaction data.

![GCP-Diagram](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289177254-f3a7ff86-92b3-46db-a156-e5ebbefc3bb9.jpg?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T201152Z&X-Amz-Expires=300&X-Amz-Signature=7f468954766df3e7ce10497cba0b489362d9fe735615fb3b20d229e901795032&X-Amz-SignedHeaders=host&response-content-type=image%2Fjpeg)


# <img width="30" alt="image" src="https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289162032-6b5ffddf-0f74-41cf-8303-22f675cabdda.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T201028Z&X-Amz-Expires=300&X-Amz-Signature=b34c3817fc897771242fe8da28d9756dd12f0e8772804a9d88da99e62e25704a&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng"> GCS

Upload the provided CSV file to your designated Google Cloud Storage (GCS) bucket. This transactional data represents a sample of real-world cases from the online food delivery industry. It includes information such as customer ID, date, time, order ID, items ordered, transaction amount, payment mode, restaurant name, order status, customer ratings, and feedback. The data showcases various scenarios, including late delivery, stale food, and complicated ordering procedures, providing valuable insights into different aspects of the customer experience.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/288459370-285dcfd6-f212-418b-b5bc-e56beb35fa52.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200951Z&X-Amz-Expires=300&X-Amz-Signature=399a5577db5c9748c64f61ec1b2fbf23df68111410fb461f295f4cdb268cb724&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)




# <img width="40" alt="image" src="https://beam.apache.org/images/mascot/beam_mascot_500x500.png"> Beam code

📖

`beam.py` code is a data processing pipeline implemented using Apache Beam. It reads data from an input file, performs cleaning and filtering operations, and writes the results to two separate BigQuery tables based on specific conditions.

The pipeline consists of the following steps:

1. Command-line arguments are parsed to specify the input file.
2. The data is read from the input file and undergoes cleaning operations, such as removing trailing colons and special characters.
3. The cleaned data is split into two branches based on the status of the orders: delivered and undelivered.
4. The total count of records, delivered orders count, and undelivered orders count are computed and printed.
5. The cleaned and filtered data from the delivered orders branch is transformed into JSON format and written to a BigQuery table.
6. Similarly, the cleaned and filtered data from the undelivered orders branch is transformed into JSON format and written to another BigQuery table.
7. The pipeline is executed, and the success or failure status is printed.

👩‍💻

Set the project in the cloud shell: `gcloud config set project your-project-id`

Install Apache Beam in the cloud shell: `pip install apache-beam[gcp]`

Give the Beam code a test run in the shell and then check the results in BigQuery:  `python beam.py --input gs://your-bucket/food_daily.csv --temp_location gs://your-bucket`

❗  Make sure that all your files and services are in the same location. E.g. both buckets should be in the same location or you will get a similar error message: 'Cannot read and write in different locations: source: US, destination: EU’


![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/288848439-5f26e09a-3b98-4848-9413-097a49a84bd6.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200851Z&X-Amz-Expires=300&X-Amz-Signature=be9cda2d17ed503988973a134b0d64fdae53c40e2ef933ac911500eb2ea6e5b9&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)



![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/288849459-8d18241f-4ede-431e-b123-744ed9470f0c.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200823Z&X-Amz-Expires=300&X-Amz-Signature=67fbcace479a4a8ff1c38314fe17ad3d71ff7a5f9cb410c65eae5bfa73728823&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

To avoid any confusion, it is recommended to delete the dataset before moving forward with actions that involve appending data in BigQuery.




# <img width="40" alt="image" src="https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289163063-c1d5bfc6-94c3-44e4-b38e-4eaf5656a840.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200756Z&X-Amz-Expires=300&X-Amz-Signature=9a2ee3210e7c42bd20c3abda11368534dfd90379cda3c375dccac6392a743385&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng"> Composer/Airflow

📖

The DAG monitors the GCS bucket for new files with the specified prefix using the GoogleCloudStoragePrefixSensor (for Airflow 1) or GCSObjectsWithPrefixExistenceSensor (for Airflow 2). When a new file is found, it executes the `list_files` function which uses the GoogleCloudStorageHook (for Airflow 1) and GCSHook (for Airflow 2) to move the file to a 'processed' subdirectory and delete the original file. Finally, it triggers the execution of a Dataflow pipeline using the DataFlowPythonOperator (for Airflow 1) or DataflowCreatePythonJobOperator/BeamRunPythonPipelineOperator (for Airflow 2) with the processed file as input.

This setup is ideal for recurring data processing workflows where files arrive in a GCS bucket at regular intervals (e.g., every 10 minutes) and need to be transformed using Dataflow and loaded into BigQuery. By using Apache Airflow and this DAG, you can automate and schedule the data processing workflow. The DAG ensures that the tasks are executed in the defined order and at the specified intervals.

Do note that the actual operator and hook names, and some of their parameters, will differ between Airflow 1 and Airflow 2. Be sure to use the correct names and parameters for your version of Airflow. For example, if your code contains `contrib` imports, it can only be run in Composer 1. 

For more information about Airflow operators, please refer to the official Apache Airflow documentation at https://airflow.apache.org/ or the Astronomer Registry at https://registry.astronomer.io/. Additionally, if you have any specific questions or need further guidance, you can interact with "Ask Astro" an LLM-powered chatbot, available at https://ask.astronomer.io.

👩‍💻

Enable Cloud Composer API, Dataflow API: `gcloud services enable composer.googleapis.com dataflow.googleapis.com`

## 🌠Composer 1 

[DataFlowPythonOperator](https://airflow.apache.org/docs/apache-airflow/1.10.5/_api/airflow/contrib/operators/dataflow_operator/index.html#airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator) can be used to launch Dataflow jobs written in Python.

To proceed, create a Composer 1 environment.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289275655-f5f7d40b-67fe-4206-9501-92b042c950f7.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200604Z&X-Amz-Expires=300&X-Amz-Signature=d372340f323e37642d9eec6225dba0bfc6b250a113e39a0e63460e386458e709&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)


 - Select n1-standard-1 (1 vCPU, 3.75 GB RAM)

 - Disk size: 30. The disk size in GB used for node VMs. Minimum is 30 GB. If unspecified, defaults to 100 GB. Cannot be updated. 

 - The Google Cloud Platform Service Account to be used by the node VMs. If a service account is not specified, the "default" Compute Engine service account is used. Cannot be updated.

Creating a Composer 1 environment typically takes around 15 minutes. If the creation process fails, you may want to consider trying a different zone.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289275675-a9bb70e4-0cef-4290-ba6a-c81e587046f9.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200542Z&X-Amz-Expires=300&X-Amz-Signature=0f131b632abfad34e4e78e5b5e47bd888fa2a129c2ccd4edd4f473b027be1425&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

The same `beam.py`, tested in the shell, can be used for both Composer 1 and Composer 2. 

Upload the `beam.py` code to the Composer bucket.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289275692-4fe512ed-489a-4955-b289-89d72be61dcf.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200451Z&X-Amz-Expires=300&X-Amz-Signature=4e3c36072da657661d0394d3eb466eefb15f1f15b283f55487503e968cf6ac06&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)


Go to the object details, copy `gsutil URI` and paste it in the DAG file (`py_file`).


![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289275723-593511d3-fde2-4704-8c3e-030037802419.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200421Z&X-Amz-Expires=300&X-Amz-Signature=051ca3734a8c193aa062244ce03b9ad71599cb89d81f5ecf8e0499bacd43379f&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289301938-8601ea31-2c88-42d6-9441-bf5576b0e19e.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200352Z&X-Amz-Expires=300&X-Amz-Signature=cd1ef30e1ebeb314d4a3fbd463fb37443cb69be6af613a330f5f51fd3e7d86fd&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

Upload `airflow.py` file to the dags folder.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289302288-91ef65c4-37de-42b4-85be-85187a4db78c.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200326Z&X-Amz-Expires=300&X-Amz-Signature=b4e1ed32a9d5c69750106a5dc32d4acc5165662c48fa2be59b590ffb69f535c7&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

After a few minutes, the DAG will appear in the Airflow UI. For testing purposes, the DAG is initially scheduled to run every 10 minutes. However, you have the flexibility to modify the schedule according to your specific requirements. Wait for the scheduled run to occur automatically or manually trigger the DAG.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289302406-52cfa6e7-e577-412c-963c-2861dc2eb4cf.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200251Z&X-Amz-Expires=300&X-Amz-Signature=30d5893f16844ac97214d5163e5aba642c4414deb512e3eeddff875b03596d9d&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289302551-e671c5ef-ed4c-470d-b96a-636119e9b847.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200220Z&X-Amz-Expires=300&X-Amz-Signature=512919283f02178d94eed10b82ef9ed96ca829d3599134ca3d751c8ba2982239&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

To gain a better understanding of the process, review the logs of each individual task. 

### 🚀 gcs_sensor

Sensor checks existence of objects: food-orders-us, food_daily. Success criteria met. Sensor found the file in the bucket.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289303033-378cd73f-d29c-43d4-9d3f-3e5cf3c754d9.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200157Z&X-Amz-Expires=300&X-Amz-Signature=50698610a63424310a41952b0bd7e73d85d718cf2d04beb0eacc39030680f6ac&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

### 🚀 list_files

Object food_daily.csv in bucket food-orders-us copied to object processed/food_daily.csv in bucket food-orders-us. Blob food_daily.csv deleted.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289303140-f1689521-8ce8-4444-b2f1-83677a6d1ac9.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200122Z&X-Amz-Expires=300&X-Amz-Signature=6e7a79814825c9286f3f1a104634a965bd82e49646de2b9a7895a7566e98bedd&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

Folder created.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289303235-9451caf3-61db-4612-915f-0e3938bef965.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200101Z&X-Amz-Expires=300&X-Amz-Signature=87d3f4b8b4543f2bd4b40a317bc145f575fec150e0bbeb2184b06cc4fca0cbf7&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

### 🚀 beamtask

The Dataflow job has just started.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289303513-03784888-c23d-43ae-9b15-f3383dd984fe.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T200026Z&X-Amz-Expires=300&X-Amz-Signature=4423bbf00e343ec26ecde1a8dd0df3ccb6730cb664281ed1398dc5d0b6964332&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)


Check the completed tasks in Dataflow.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289303579-922be238-59f7-413e-9415-f6cda9b772f5.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T195955Z&X-Amz-Expires=300&X-Amz-Signature=23d89ad6dd1678e99cfeb1423bdda87f3dc567f425aea31a3fb23e42d65f3d48&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289303648-cef67aea-f2ce-4d7a-8047-5a46cc237758.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T195924Z&X-Amz-Expires=300&X-Amz-Signature=80ac3e987b88243309e3b05e9dd7839a5a3c1aa8faa6f80c99fae9e51b113d97&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

Open BigQuery to see the results.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289303774-eebb4d73-6332-4a90-8798-e81cc5b2f628.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T195849Z&X-Amz-Expires=300&X-Amz-Signature=26841697ba0db73977b81a0c8671a4b0de6463913596e3e1b455e3f45c5b4e92&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

In practice, files often come with timestamps. As a test, I have uploaded a new file to the bucket to verify if the solution is functioning correctly.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289304513-6a16b07e-7bc3-4522-9fb6-ff94347ffa63.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T195708Z&X-Amz-Expires=300&X-Amz-Signature=16a7d49e7a5f2943afd8992854ea59baf7fdc81da63a1ba6df4c83062d7cb24f&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)


The solution performed as expected. The new file was successfully copied to the 'processed' folder, and the same process was repeated. The resulting transformed data will be appended to the existing tables in BigQuery.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289304129-a4d449d0-35ec-42dc-b6b6-c08e91934b95.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T195634Z&X-Amz-Expires=300&X-Amz-Signature=045b2c185f564bd95bb1b4b806a295977ad9f3d778b27a2c0101d4ba5e5599e6&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289304268-12d734e3-d0ae-44ff-9bd6-f1e8b27859e1.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T195607Z&X-Amz-Expires=300&X-Amz-Signature=984ab81b610d8b26814ad4c1a19a3501065a8dfe8770db99b967a9d3944177b5&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

The values can be accessed and retrieved from XComs.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289304361-1e26c216-0a18-475d-98eb-367883d7469a.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T194608Z&X-Amz-Expires=300&X-Amz-Signature=57367dede72e88e80ff225a284230b51baa1f5398f6e4e10ed3088a266624015&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)



## 🌠Composer 2

Let's move to Composer 2. Create a Composer 2 environment. 

The DAGs feature two operators: [DataflowCreatePythonJobOperator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/dataflow/index.html#airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator) and [BeamRunPythonPipelineOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-beam/stable/operators.html#python-pipelines-with-dataflowrunner). While the former is deprecated and no longer actively maintained, it is still available and functional. Although it is recommended to use the Beam operator for improved functionality and ongoing support, you can still try the deprecated operator.


![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289351349-9d918734-cd98-49eb-a973-586d6178d341.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T194533Z&X-Amz-Expires=300&X-Amz-Signature=1c2d856a539c871064e09b77c9f2e49cdf156e661b922ffdea4b4152fa93c111&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)


❗ It's important to give `Cloud Composer v2 API Service Agent Extension` role to your Service Account.

Select Environment size: Small.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289386394-64581910-e4e2-4196-8812-4478a5af0739.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T194431Z&X-Amz-Expires=300&X-Amz-Signature=f1b6343f007ac9fd54c76d1df24c51789daed18407d8139b7733912a29a4916e&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)



The rest is the same, upload CSV file to the bucket, add Beam code to the Composer bucket, copy `gsutil URl` link and add it to the DAG.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289386469-a7198bbd-db3c-49b3-b8d8-dd7b6e50d690.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T194159Z&X-Amz-Expires=300&X-Amz-Signature=67c3779a38e4264b15ace88f0226dcc8f8ee1786eb66e90ea29eb04f4b76a0d4&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)


![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289386524-22293adc-98b2-42ba-932e-e565867b7862.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T194226Z&X-Amz-Expires=300&X-Amz-Signature=5f1c7a405df1470703a973c1b6c5e29a67003f9a85d038987090c82331ac8736&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)


![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289386563-c4c5018d-7eb3-448e-875c-7c975c1200a9.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T194257Z&X-Amz-Expires=300&X-Amz-Signature=93daae52b64b8f4755427ba384e3143af7f4d94a0f87e1d0ba134c31e570104e&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)



![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289386333-f28d8fa7-84f1-4e0a-8184-575aeb9e2ece.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T194328Z&X-Amz-Expires=300&X-Amz-Signature=a0ad8af38010bbdff6151886c90c823d8207c61734bdc276485ef01b519ab512&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)




Upload `airflow2.py` code to the dags folder (with DataflowCreatePythonJobOperator or BeamRunPythonPipelineOperator).


Wait for the DAG to appear in the Airflow UI.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289386855-2867c0f1-922e-48c8-a657-7563a9afbdcb.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T194126Z&X-Amz-Expires=300&X-Amz-Signature=50aef8c707f3f47a5428d398cd04b45acbd8b6b90ee2e8a04fa78fd132c32488&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

Operators will be visible in the Graph section.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289813523-1e6cc0bb-28a2-497c-be2d-a73a27a99d9b.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T194045Z&X-Amz-Expires=300&X-Amz-Signature=ac2ee9dc2d026ec56c741d48dd80566ca9c1547e0e3e4e1ee3bd332f191e2b7e&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)


![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289813658-42c60726-f7da-4e0c-8d88-161a69e4fc69.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T193959Z&X-Amz-Expires=300&X-Amz-Signature=c0b7bddfc85f33e24e525e7a98054581c36373cded8ba6eeaae219e7571415fe&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)



Since it is scheduled to run '@daily' this time, I manually triggered it.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289386866-4b889475-d7ab-4c62-81eb-97f153b2bb91.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T193232Z&X-Amz-Expires=300&X-Amz-Signature=ba01be4a3b57cdbbe72d4e400e1ecacab95a14ae280e375349caefd753370f80&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

Open Dataflow to check if the job is currently running.

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/289386894-4fe108f0-ddce-458e-83fa-159a4860f186.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T193315Z&X-Amz-Expires=300&X-Amz-Signature=5631976018a11e0e80de018b2c9fd4b567303ce33877916fb20e513ba0876a0e&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)

As expected, the Dataflow job will create 2 tables in BigQuery. 👏🎉


❗ Make sure to delete Composer from your setup as it can be a costly service. It's worth mentioning that Google Cloud provides an advantageous [Free Trial](https://cloud.google.com/free/docs/free-cloud-features#free-trial) option. As a new customer, you will receive $300 in free credits, allowing you to thoroughly explore and assess the capabilities of Google Cloud without incurring any additional expenses.

# <img width="30" alt="image" src="https://seeklogo.com/images/G/google-looker-logo-B27BD25E4E-seeklogo.com.png"> Looker

Connect to your Looker account: https://lookerstudio.google.com. Select BQ connection.
Create your own daily report, use delivered/other_status_orders tables. Here is my example

![image](https://github-production-user-asset-6210df.s3.amazonaws.com/83917694/288892124-ebca078f-f231-4f86-9938-c0cf026c0c97.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20260706%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20260706T193431Z&X-Amz-Expires=300&X-Amz-Signature=053fdbed2f4a6e369f55a9a2094fff7a3ca71b628f317d43ffff6fa62887d61f&X-Amz-SignedHeaders=host&response-content-type=image%2Fpng)
