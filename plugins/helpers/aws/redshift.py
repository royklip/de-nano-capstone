import json
import time

from helpers.aws.aws_service import AwsService


class Redshift(AwsService):
    def __init__(self, credentials, region: str):
        """ Initialize the redshift class. """  
        super(Redshift, self).__init__(credentials, region)

        # Create AWS clients and resources
        self.iam = self.session.client('iam')
        self.redshift = self.session.client('redshift')
        self.ec2 = self.session.resource('ec2')


    def create_iam_role(self, name: str) -> None:
        """ Create IAM role for the redshift cluster. """
        try:
            self.iam.create_role(
                Path='/',
                RoleName=name,
                Description='Allows access to S3 cluster',
                AssumeRolePolicyDocument=json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "redshift.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                        }
                    ]
                })
            )
        except self.iam.exceptions.EntityAlreadyExistsException:
            pass
        except Exception as e:
            print(e)

        # Attach the S3 read access policy
        try:
            self.iam.attach_role_policy(RoleName=name, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
        except Exception as e:
            print(e)   


    def delete_iam_role(self, name: str) -> None:
        """ Detach all policies and delete IAM role. """
        attached_role_policies = self.iam.list_attached_role_policies(RoleName=name)

        try:
            for policy in attached_role_policies['AttachedPolicies']:
                self.iam.detach_role_policy(RoleName=name, PolicyArn=policy['PolicyArn'])
            self.iam.delete_role(RoleName=name)
        except Exception as e:
            print(e)


    def create_cluster(self, 
            cluster_id: str, 
            cluster_type: str,
            node_type: str,
            num_nodes: int,
            db_name: str,
            db_user: str,
            db_password: str,
            role_name: str,
        ) -> None:
        """ Create redshift cluster. """
        try:
            self.redshift.create_cluster(
                ClusterIdentifier=cluster_id,
                ClusterType=cluster_type,
                NodeType=node_type,
                NumberOfNodes=num_nodes,
                DBName=db_name,
                MasterUsername=db_user,
                MasterUserPassword=db_password,
                IamRoles=[
                    self._get_iam_role_arn(role_name)
                ]
            )
        except Exception as e:
            print(e)

        cluster_props = self.get_cluster_props(cluster_id)

        # Wait for the cluster to be available
        while cluster_props['ClusterStatus'] != 'available':
            time.sleep(5)
            cluster_props = self.get_cluster_props(cluster_id)
        else:
            return cluster_id


    def _get_iam_role_arn(self, name: str) -> str:
        """ Get Arn of role with 'name'. """
        try:
            role = self.iam.get_role(RoleName=name)
            return role['Role']['Arn']
        except Exception as e:
            print(e)

    
    def get_cluster_props(self, cluster_id: str) -> dict:
        """ Get redshift properties of cluster with cluster_id. """
        return self.redshift.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]


    def create_access_port(self, cluster_id: str, db_port: int) -> None:
        """ Create open TCP port to access cluster. """
        try:
            defaultSg = self._get_default_sg(cluster_id)
            ingress = self._get_ingress(defaultSg, db_port)

            # if not self._security_group_permission_exists(defaultSg.ip_permissions, db_port):
            defaultSg.authorize_ingress(**ingress)
        except Exception as e:
            print(e)


    def delete_access_port(self, cluster_id: str, db_port: int):
        try:
            defaultSg = self._get_default_sg(cluster_id)
            ingress = self._get_ingress(defaultSg, db_port)
            defaultSg.revoke_ingress(**ingress)
        except Exception as e:
            print(e)


    def _get_default_sg(self, cluster_id: str):
        cluster_props = self.get_cluster_props(cluster_id)
        if cluster_props['ClusterStatus'] != 'available':
            raise Exception('Cluster is not available (yet)')

        vpc = self.ec2.Vpc(id=cluster_props['VpcId'])
        return list(vpc.security_groups.all())[0]


    def _get_ingress(self, defaultSg, db_port: int):
        return {
            'GroupName': defaultSg.group_name,
            'CidrIp': '0.0.0.0/0',
            'IpProtocol': 'TCP',
            'FromPort': db_port,
            'ToPort': db_port
        }


    def delete_cluster(self, cluster_id: str) -> None:
        """ Delete redshift cluster. """
        try:
            self.redshift.delete_cluster(ClusterIdentifier=cluster_id,  SkipFinalClusterSnapshot=True)
        except Exception as e:
            print(e)
