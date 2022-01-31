from helpers.aws.aws_service import AwsService


class Emr(AwsService):
    def __init__(self, credentials, region: str):
        """ Initialize the EMR class. """
        super(Emr, self).__init__(credentials, region)

        # Create an EMR client
        self.emr = self.session.client('emr')


    def create_cluster(self, job_flow: dict) -> None:
        try:
            self.emr.run_job_flow(job_flow)
        except Exception as e:
            print(e)
