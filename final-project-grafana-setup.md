# Final Project - EC2 Launch Docker/Grafana on Startup & Save Changes

Each team will need to make a Virtual Machine (EC2) in AWS - you will then set it up to run docker for you, and docker to run the Grafana image.

Read this whole file all the way through before starting any of the steps!


###    chmod 400 nubi-key.pem
## SSH instance link 'ssh -i "nubi-key.pem" ec2-user@ec2-34-253-231-170.eu-west-1.compute.amazonaws.com'


## EC2 Setup

**Important** - _Nominate one person to do the setup - they will end up with the SSH key._

### Security group for your EC2

Before creating your own EC2 instance, you will need to create a [security group]

BUT_ - When making the security group, do this:

1. Use your team name e.g. `your-team-sg`
1. Limit all Inbound access to your teams IP addresses only
    1. For each person in your team:
        1. Select `SSH` for `Type` and `My IP` for `Source` (to make a rule for port 22)
        1. Select `HTTP` for `Type` and `My IP` for `Source` (to make a rule for port 80)

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


### EC2 Instance Setup

Now let's set an instance up.

1. Go to EC2 and select `Launch Instance`
1. In the `Names and tags` section,
    1. add a name for your EC2 instance ( e.g `Your-Name Web Server`)
1. In the `Application and OS Images` section,
    1. select `Amazon Linux 2023 AMI`
1. In the `Instance type` section,
    1. select an instance type of `t2.micro`
1. In the `Key pair` section,
    1. Click on the `Create new key pair` link, a pop up dialogue will appear
    1. Do so by entering a key pair name (e.g  `yourname-key`)
    1. Keep key file format as .pem and click the `Create key pair` button
    1. The key will download automatically to you downloads folder
    1. The popup will close
    1. Move the downloaded key file `yourname-key.pem` into a suitable directory
        1. **DO NOT** put this in any Git folder - this would be like adding a password to git, but worse, which is **VERY BAD**
    1. The name should autofill in the `Key pair` section
1. In the `Network Settings` section,
    1. Click the `Edit` button on the right hand side
    1. Change the `VPC` to `RedshiftVPC`
    1. And use the Subnet dropdown to change it to the `RedshiftPublicSubnet0` subnet
    1. Under `Auto-assign Public IP`, select `Enable`
    1. Under `Firewall(security group)`, select `Existing security group`
    1. Under `Firewall(security group)`, use the Security groups dropdown to select the security group, you created earlier (e.g `your-name-sg`)
1. In the `Configure storage` section,
    1. Click the `Advanced` link on the top right
    1. Click the drop-down arrow next to `Volume 1`
    1. Under `Encrypted` select `Encrypted` from the dropdown
    1. Leave everything else as is!
1. In the `Advanced details` section,
    1. Change the `IAM Instance profile` to `ec2-role`
    1. Do not touch any other settings here
1. Click on the orange button on the right hand side, to `Launch instance`
1. Navigate to `Instances` and select the `Instance ID` value of your instance
1. Wait for your instance to have an instance state of `Running` before moving on
    1. This should only take about 30 seconds

### Accessing the Instance

Your instance has now been spun up and is ready to be accessed. Let's see how we can go about getting inside it:

1. On your instance summary page, select `Connect` in the top-right of the webpage
1. Select the `SSH Client` tab and copy the long `ssh` command under `Example:`

Now follow the below steps on your terminal (use `wsl` if on Windows):

1. Open a terminal in the folder your downloaded key file is in e.g. `yourname-key.pem`
1. Run: `chmod 400 {name-of-key}.pem`
1. Paste the `ssh` command you copied and hit enter
1. You will be asked `Are you sure you want to continue connecting (yes/no/[fingerprint])?`, type `yes` and hit enter
1. You should now be logged in!
    1. Your terminal prompt should change to show you are inside the instance!

1. Use the most recent AWS "Amazon Linux 2023" machine image (AMI)
1. SSH into the instance with the SSH key you downloaded. DO NOT LOSE THIS KEY!
    1. **DO NOT** put this in any Git folder - this would be like adding a password to git, but worse, which is **VERY BAD**
1. Then to install docker inside it you need to run this:
    if on Linux / Mac, run these sudo commands. If on Windows, run the terminal as administrator, then run the same commands without `sudo`

    ```sh
    sudo yum install docker -y
    sudo service docker start
    sudo usermod -a -G docker ec2-user
    sudo chkconfig docker on
    ```

---

## Docker Setup

The following steps are to ensure any changes you've made in Grafana are saved when you Stop/Pause your instance and Start it again.

- Connect/SSH into your EC2 Instance.
- Ensure your Grafana Docker Image is **_not_** running. To check this, run the command:
    - `docker ps -a`
- If it is running, use this to stop it and remove it:
    - `docker stop <container-id>`
    - `docker rm <container-id>`
- Create a docker volume by running the following command:
    - `docker volume create grafana-storage`
- Verify the docker volume has been created by running command:
    - `docker volume ls`
- Verify you can run your Grafana image/container with volume by running this command:

    ```sh
    sudo docker run -d -p 80:3000 --rm \
        --volume grafana-storage:/var/lib/grafana grafana/grafana ;
    ```

- Check the container is running with `docker ps -a`
- Also check the Grafana site is running by going to the Instance's public IP address (check the EC2 page)
    - e.g <http://12.34.56.78:80> (use your IP address)
    - You don't log in yet (See steps later for that)
- Now go to AWS and click `Stop Instance` to stop the instance
- From the EC2 Instance list, select your instance and click the `Actions` drop-down -> `Instance settings` -> `Edit user data`
- In the `Edit user data` page, ensure `Modify user data as text` is selected and then copy & paste the following into the text field:

```yaml
Content-Type: multipart/mixed; boundary="//"
MIME-Version: 1.0

--//
Content-Type: text/cloud-config; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="cloud-config.txt"

#cloud-config
cloud_final_modules:
- [scripts-user, always]

--//
Content-Type: text/x-shellscript; charset="us-ascii"
MIME-Version: 1.0
Content-Transfer-Encoding: 7bit
Content-Disposition: attachment; filename="userdata.txt"

#!/bin/bash
sudo docker run -d -p 80:3000 --rm --volume grafana-storage:/var/lib/grafana grafana/grafana
--//--
```

- Click `Save`
- Start your Instance again
- Docker should now be running automatically
- Verify this by logging back in and running `docker ps -a`
- Also verify it by browsing to the Grafana site via the Instance's public IP address, and check the changes you made are there, e.g <http://12.34.56.78:80>
    - You don't need to log into it yet

---

## Grafana setup

- Browse to it by going to the Instance's public IP address (check the EC2 page)
    - e.g <http://12.34.56.78:80> (use your IP address)
- Log into Grafana with user `admin` and password `admin`
- Change the admin password to a _secure_ one, and put it on your teams _private_ Slack or MS Teams channel

### Setup Grafana users

To create a new user login for each team member, navigate to `Server Admin --> Users --> New user` and begin creating unique users with _secure passwords_.

### Connecting Grafana to CloudWatch

Just like in the earlier exercise, we need to connect a data source in order to generate some graphs and metrics.

1. In Grafana, navigate to `Configuration --> Data Sources`. Select `Add data source`, search for `CloudWatch` and select.
1. Give it a name, or leave as default.
1. Leave other settings as default.
1. Set region to `eu-west-1`.
1. Select `Save & Test`. You should see a confirmation `Data source is working`.

### Creating a Lambda metric

We can make graphs and metrics for our Lambda - e.g. how many time it ran, how long it took, how many errors there were.

1. Create a new dashboard and add a new panel.
1. Select `CloudWatch` as the query type, and `CloudWatch Metrics` as the query mode.
1. Select `AWS Lambda` as the namespace, and `Invocations` as the metric name.
1. Add a new Dimension. Select `Function name` as the resource and select the dimension value as your teams ETL lambda.
1. Update the time query to be last 24 hours or 2/7 days if you need to go back that far to see data being graphed.

You should be able to now see how many times your lambda has been invoked over the time elapsed configured for the time period. You can also choose different metric options to suit your needs. For example, you can select `Error` and `Duration` as the metric name, as well as different stats such as `Average`, `Sum`, `Min` and `Max`.

As team, think about what kind of monitoring metrics you can establish to display on your new dashboard.

### Connecting Grafana to Redshift

- Install the "Amazon Redshift" plugin
- Add a "Redshift" datasource
- Leave the _assumed role_ blank (as it defaults to the role on EC2 instance)
- Set default region to `eu-west-1`
- Select the redshift cluster in "Cluster Identifier"
- Add your team's database user and database name

You can now create dashboards by using the data from your Redshift database, using SQL.

As a team think about what data from Redshift you want to display, for example, revenue per day or week, number of items sold per day, number of each type of drink sold in the last week, and so on. You wil have to create the SQL for this yourselves.### 




