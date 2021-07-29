import os
import sys
import boto3

class awsBoto3:
    def __init__(self):
        client = boto3.client('emr')


    def fetch_emr_details(self, cluster_id):
        self.cluster_id = cluster_id
        response = self.client.list_instances(
            ClusterId=self.cluster_id,
            InstanceStates=[
                'RUNNING',
            ],
        )
        #Get list of ip addresses.

        self.list_ips = []
        #print(response['Instances'][len(list_intance_resp['Instances']) - 1]['PrivateIpAddress']
        for i in range(2):
            self.list_ips.append(response["Instances"][i]["PrivateIpAddress"])

        return self.list_ips

