#!/bin/bash

# Specify the desired volume size in GiB as a command-line argument. If not specified, default to 20 GiB.
SIZE=${1}

# Update tools.
sudo yum update -y && \
curl -O https://bootstrap.pypa.io/get-pip.py && \
python3 get-pip.py --user && \
pip3 install --upgrade --user awscli pip aws-sam-cli

# Install the jq command-line JSON processor.
sudo yum install -y jq 2&> /dev/null

# Get the ID of the envrionment host Amazon EC2 instance.
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)

# Get the ID of the Amazon EBS volume associated with the instance.
VOLUMEID=$(aws ec2 describe-instances \
  --instance-id $INSTANCEID | \
  jq -r .Reservations[0].Instances[0].BlockDeviceMappings[0].Ebs.VolumeId)

# Resize the EBS volume.
aws ec2 modify-volume --volume-id $VOLUMEID --size $SIZE
