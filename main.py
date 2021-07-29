import sys
import os
from aws_boto3 import *
import subprocess

def fetch_ip_address(cluster_id):
    #get boto3 emr client
    obj_awsBoto3 = awsBoto3()

    #fetch details of emr cluster
    list_ips = obj_awsBoto3.fetch_emr_details(cluster_id)

    #return ip addresses.
    return list_ips


def ssh_and_update_cron(list_ips):
    print(list_ips)
    for i in list_ips:
        #SSH into the machine
        if str(i) == "172.30.138.8":
            print("SSH into " + str(i))
            process = subprocess.Popen("ssh ec2-user@" + i + " ls", shell=True,
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            output, stderr = process.communicate()
            status = process.poll()
            print(output)


def configure_ssh_in_emr(cluster_id):
    #Get all IP Addresses of a certain EMR Cluster
    list_ips = fetch_ip_address(cluster_id)
    #SSH into every IP Address and Add or Update the Cronfile
    ssh_and_update_cron(list_ips)


try:
    cluster_id = sys.argv[1]
    configure_ssh_in_emr(cluster_id)
except Exception as e:
    print(str(e))