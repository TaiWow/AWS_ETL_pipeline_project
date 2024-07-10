# Final-project
## About

This project is providing a service to *Super Cafe* in which we, **Nubi**, will Extract, Transform and Load data provided by the client into a database. Next...

## Elevator pitch:
*FOR* the super cafe owner 

*WHO* need their data data organised by:
* Sales
* Performance
* Products
* Stores

*THE* NUBI service 

*IS* a data engineering service 

*THAT* creates structure, organisation and security to build clear reports using data visualisations

*UNLIKE* manual processing of data and having separate disjointed forms of data

*OUR SERVICE* automates data organisation and visualisation offering real time insights into sales and operations. 
Our service ensures customer privacy (names, card number, etc.)


## Team Members

* Haaris H
* Byron S
* Taiwo A
* Mujtaba M

## Database Schema Model

![database_schema_model](database_schema_model.png)


# Sprint 1 - ETL

In this sprint we complete the first stages of our project. By implementing ETL (Extract, Transform and Load) we stored data provided by the client into a Postgres SQL database.

To run ETL using files provided:

 1. Copy and Paste "Nubi_setup.sql" into adminer
 2. Run load.py

# Sprint 2

 In this sprint we have set up an S3 bucket to store our data. We then ran a Lambda function which consisted of a modfified version of our ETL from last sprint to run the process in the cloud. This ETL loads our data into Amazon Redshift where client data will be stored.

 To run ETL in AWS:

 1. Create S3 bcuket and Lambda function (Configuration details followed from class exercise)
 2. Paste Nubi_Redshift_Setup.sql (doesnt exist yet) into Redshift query to setup databse tables
 3. Run modified ETL function via Lambda to Extract from S3, Transform and Load into Redshift
 4. Data should appear in Redshift...


 # Sprint 3 

 In this sprint 
 ### Security Group Setup

Before creating your own EC2 instance, you will need to create a [security group](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/working-with-security-groups.html). Security groups take control of the traffic that is allowed in and out of your instance. You can apply restrictions on port ranges and IP ranges. We will be restricting `SSH` access to your IP, but open `HTTP` to the world. This is bad practice, and so you would normally be much more restrictive in terms of what you allow in and out, but for the sake and simplicity of this exercise, we won't need to worry about that.

1. Go to `EC2` page by using the search bar
1. On the left-hand side under `Network & Security`,
    1. select `Security Groups`
    1. and then select `Create security group`
1. Give your security group a unique name (e.g. `your-name-sg`) and a description (e.g. `Your-Name SG`)
1. Change the VPC to the `RedshiftVPC`
    1. Delete the contents of the `VPC` box - it should then offer you a dropdown list - select `RedshiftVPC`
    1. If not, type `Red` in the box - it should find the one named `RedshiftVPC`
1. Under `Inbound rules`, select `Add rule`
    1. Rule 1: Select `SSH` for `Type` and `My IP` for `Source`
    1. Rule 2: Select `HTTP` for `Type` and input `0.0.0.0/0` in the text field to the right of `Source` and `Anywhere-IPv4` for `Source`
1. Under `Outbound rules`,
    1. Rule 1: Select `HTTP` as the type and input `0.0.0.0/0` in the text field to the right of `Destination`
    1. Rule 2: Select `HTTPS` as the type and input `0.0.0.0/0` in the text field to the right of `Destination`
1. Under the Tags section add a tag with key `Name` and value `your-name-sg`
1. Select `Create security group` to finish


## Accessing the Instance

- SSH Connection - on your instance summary page, select Connect and copy the SSH command.
- Terminal Setup - Open a terminal in the directory containing your .pem file.
- Run -  'chmod 400 yourname-key.pem'
- Paste the SSH command and connect.
- confirm the connection by typing yes when prompted.
- terminal prompt should change to show you are inside the instance! ready to use it.

## Docker Setup

Run the following commands:The following steps are to ensure any changes you've made in Grafana are saved when you Stop/Pause your instance and Start it again.

SSH/Connect into the EC2 instance 

```sh
sudo yum install docker -y
sudo service docker start
sudo usermod -a -G docker ec2-user
sudo chkconfig docker on
```
*Configure Docker Volume ensure the Grafana Docker image is not running*

```sh
docker ps -a
docker stop <container-id>
docker rm <container-id>
```

*Create a Docker volume*

```sh
docker volume create grafana-storage
```
*verify docker volume has been created*

```sh
docker volume ls
```
*Run the Grafana container*

```sh
sudo docker run -d -p 80:3000 --rm --volume grafana-storage:/var/lib/grafana grafana/grafana
```
