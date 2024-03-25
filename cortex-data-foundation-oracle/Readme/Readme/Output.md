**Cortex Framework for Oracle EBS**

**Deployment for Development or Production environments**

Navigate into the folder

```
cd oracle-data-foundation
```
**Deployment steps**

These are the steps for deployment:

1. Prerequisites

2. Establish project and dataset structure

3. Configure Cloud Platform Components

4. Configure deployment

5. Execute deployment

6. Test, customize, prepare for upgrade

**Prerequisites**

Understand the Framework

A successful deployment depends on a good understanding of:

- Your company's business rules and requirements

- Functional understanding of the workload

- Google Cloud fundamentals and products

Before continuing with this guide, make sure you are familiar with:

- Google Cloud Platform [fundamentals](https://www.cloudskillsboost.google/course_templates/60)

- How to navigate the [Cloud Console](https://cloud.google.com/cloud-console), [Cloud Shell](https://cloud.google.com/shell/docs/using-cloud-shell) and [Cloud Shell Editor](https://cloud.google.com/shell/docs/editor-overview)

- Fundamentals of [BigQuery](https://cloud.google.com/bigquery/docs/introduction)

- General navigation of [Cloud Build](https://cloud.google.com/build/docs/overview)

- Fundamentals of [Identity and Access Management](https://cloud.google.com/iam/docs/)

- Fundamentals of [Cloud Composer](https://cloud.google.com/composer/docs/concepts/overview) or [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html)

**Establish project and dataset structure**

You will require at least one GCP project to host the BigQuery datasets and execute the deployment process.

This is where the deployment process will trigger Cloud Build runs. Workload should have the Stage, Ods and Reporting dataset (Edw).

![Enter image alt description](Images/zjh_Image_1.png)

**Data Integration overview**

You will need to identify:

- The Google Cloud Project where the data is stored, which will be accessed by technical practitioners for data model consumption.

- The Raw BigQuery Dataset (Stage), where the source data is replicated.

- The Ods BigQuery Dataset, where the latest available records from the CDC processing of data land.

- The BigQuery reporting dataset (Edw).

## **Configure Google Cloud Platform components**

### **Enable Required Components**

The following Google Cloud components are required:

- Google Cloud Project

- BigQuery instance and datasets

- Service Account with Impersonation rights

- Cloud Storage Buckets

- Cloud Composer

**Grant permissions to the executing user**

If an individual is executing the deployment with their own account, they will need, at minimum, the following permissions in the project where Cloud Build will be triggered:

- Service Usage Consumer

- Storage Object Viewer for the Cloud Build default bucket or bucket for logs

- Object Writer to the output buckets

- Cloud Build Editor

- Project Viewer or Storage Object Viewer

**Create a Storage bucket for storing DAG related files**

A storage bucket will be required to store DAG scripts and generated  logs during deployment. These scripts will have to be manually moved into a Cloud Composer or Apache Airflow instance after deployment.

Navigate to Cloud Storage and create a bucket in the same region as your BigQuery datasets.

Alternatively, you can use the following command to create a bucket from the Cloud Shell:

```
gsutil mb -l <REGION/MULTI-REGION> gs://<BUCKET NAME>
```
## **Configure Deployment**

The behavior of the deployment is controlled by the configuration file** config.json** .

The file contains global configuration and configuration specific to workload.

![Enter image alt description](Images/Ryj_Image_2.jpeg)

Open the file in** config/config.json. **From the Cloud Shell:

```
edit config/config.json
```
## **Execute Deployment**

This step requires config.json to be configured as described in the section Configure Deployment.

Run the Build command with the target log bucket.

```
gcloud builds submit --project <execution project> \    --substitutions=_GCS_BUCKET=<Bucket for logs - Cloud Build Service Account needs access to write here>
```
You can follow the main Build process from the first link of logs:

![Enter image alt description](Images/bAF_Image_3.jpeg)

Alternatively, if you have enough permissions, you can see the progress from [Cloud Build](https://console.cloud.google.com/cloud-build/).

![Enter image alt description](Images/6qQ_Image_4.png)

## **Move the files into the DAG bucket**

```
gsutil -m cp -r  gs://<output bucket>/dags/ gs://<composer dag bucket>/
```