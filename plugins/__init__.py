from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

class CapstonePlugin(AirflowPlugin):
    name = "capstone_plugin"
    operators = [
        operators.CleanDataOperator,
        operators.CreateRedshiftClusterOperator,
        operators.CreateRedshiftConnectionOperator,
        operators.DeleteRedshiftClusterOperator,
        operators.CreateEmrConnectionOperator,
        operators.DataExistsOperator
    ]
    helpers = [
        helpers.AwsService,
        helpers.Redshift,
        helpers.S3,
        helpers.DataCleaner
    ]
