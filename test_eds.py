import datetime
from unittest.mock import Mock, MagicMock
import importlib
import json
import mock
import os
import pytest

# Region needs to be set before importing eds
from _pytest.monkeypatch import MonkeyPatch

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
os.environ["SKIP_LOADING_KUBE_CONFIG"] = "YES"
import eds

from mock import Mock
from kubernetes import client, config

create_namespaced_deployment = 'eds.appsV1Api.create_namespaced_deployment'
v1_job = "eds.client.V1Job"
example_metadata_string = "{\"metadata\": {\"id\": \"12\", \"date\": \"2020-06-15 20:59:38\", \"restaurantIds\": [\"310\"], \"pod\": \"evd-batch-data-upload\", \"namespace\": \"evd\", \"deploymentGroupId\": \"71\"}}"
example_deployment_string = "{\"apiVersion\": \"apps/v1\", \"kind\": \"Deployment\", \"metadata\": {\"name\": \"evd-batch-data-upload\"}, \"spec\": {\"selector\": {\"matchLabels\": {\"app\": \"evd-batch-data-upload\"}}, \"replicas\": 1, \"template\": {\"metadata\": {\"labels\": {\"app\": \"evd-batch-data-upload\"}}, \"spec\": {\"containers\": [{\"name\": \"evd-batch-data-upload\", \"image\": \"artifactory.bre.mcd.com/docker/evd-batch-data-upload:12\", \"volumeMounts\": [{\"name\": \"evd-batch-data-upload-secrets\", \"mountPath\": \"/secrets\"}, {\"name\": \"evd-batch-data-upload-image-data-volume\", \"mountPath\": \"/image-data\", \"subPath\": \"app1\"}]}], \"volumes\": [{\"name\": \"evd-batch-data-upload-secrets\", \"secret\": {\"secretName\": \"evd-batch-data-upload\"}}, {\"name\": \"evd-batch-data-upload-image-data-volume\", \"persistentVolumeClaim\": {\"claimName\": \"evd-batch-data-upload-image-data-volume\"}}]}}}}"
patch_request_str = 'requests.patch'

mock_deployment_sqs_message = {
    "Messages": [
        {
            "MessageId": "mock-message-id",
            "ReceiptHandle": "mock-receipt-handle",
            "Body": "Pushed deployment message to S3 bucket with name of bred-eds-docker for restaurant 1 for application bred-eds-docker",
            "MessageAttributes": {
                "metadata": {
                    "DataType": "String",
                    "StringValue": example_metadata_string
                },
                "deployment": {
                    "DataType": "String",
                    "StringValue": example_deployment_string
                }
            }
        }
    ]
}

mock_persistentvolume_sqs_message = {
    "Messages": [
        {
            "MessageId": "mock-message-id",
            "ReceiptHandle": "mock-receipt-handle",
            "Body": "mock-body",
            "MessageAttributes": {
                "metadata": {
                    "DataType": "String",
                    "StringValue": example_metadata_string
                },
                "persistentvolume": {
                    "DataType": "String",
                    "StringValue": "{\"apiVersion\": \"v1\", \"kind\": \"PersistentVolume\", \"metadata\": {\"name\": \"evd-batch-data-upload-image-data-volume\"}, \"spec\": {\"storageClassName\": \"cephfs\", \"accessModes\": [\"ReadWriteMany\"], \"capacity\": {\"storage\": \"2Gi\"}, \"flexVolume\": {\"driver\": \"ceph.rook.io/rook\", \"fsType\": \"ceph\", \"options\": {\"clusterNamespace\": \"rook-ceph\", \"fsName\": \"ceph-filesystem\"}}, \"persistentVolumeReclaimPolicy\": \"Recycle\"}}"
                }
            }
        }
    ]
}

mock_persistentvolumeclaim_sqs_message = {
    "Messages": [
        {
            "MessageId": "mock-message-id",
            "ReceiptHandle": "mock-receipt-handle",
            "Body": "mock-body",
            "MessageAttributes": {
                "metadata": {
                    "DataType": "String",
                    "StringValue": example_metadata_string
                },
                "persistentvolumeclaim": {
                    "DataType": "String",
                    "StringValue": "{\"apiVersion\": \"v1\", \"kind\": \"PersistentVolumeClaim\", \"metadata\": {\"name\": \"evd-batch-data-upload-image-data-volume\"}, \"spec\": {\"storageClassName\": \"cephfs\", \"accessModes\": [\"ReadWriteMany\"], \"resources\": {\"requests\": {\"storage\": \"1Gi\"}}}}"
                }
            }
        }
    ]
}

mock_cronjob_sqs_message = {
    "Messages": [
        {
            "MessageId": "mock-message-id",
            "ReceiptHandle": "mock-receipt-handle",
            "Body": "mock-body",
            "MessageAttributes": {
                "metadata": {
                    "DataType": "String",
                    "StringValue": example_metadata_string
                },
                "cronjob": {
                    "DataType": "String",
                    "StringValue": "{\"spec\": {\"jobTemplate\": {\"spec\": {\"template\": {\"spec\": {\"containers\": [{\"image\": \"test-image:test\"}]}}}}}}"
                }
            }
        }
    ]
}

mock_certificate_sqs_message = {
    "Messages": [
        {
            "MessageId": "mock-message-id",
            "ReceiptHandle": "mock-receipt-handle",
            "Body": "mock-body",
            "MessageAttributes": {
                "metadata": {
                    "DataType": "String",
                    "StringValue": example_metadata_string
                },
                "certificate": {
                    "DataType": "String",
                    "StringValue": "{\"foo\": \"bar\"}"
                }
            }
        }
    ]
}

mock_certificatedelivery_sqs_message = {
    "Messages": [
        {
            "MessageId": "mock-message-id",
            "ReceiptHandle": "mock-receipt-handle",
            "Body": "mock-body",
            "MessageAttributes": {
                "metadata": {
                    "DataType": "String",
                    "StringValue": example_metadata_string
                },
                "certificatedelivery": {
                    "DataType": "String",
                    "StringValue": "{\"metadata\": {\"name\": \"evd-batch-data-upload\"}}"
                }
            }
        }
    ]
}

mock_sqs_message_no_metadata = {
    "Messages": [
        {
            "MessageId": "mock-message-id",
            "ReceiptHandle": "mock-receipt-handle",
            "Body": "Pushed deployment message to S3 bucket with name of bred-eds-docker for restaurant 1 for application bred-eds-docker",
            "MessageAttributes": {
                "deployment": {
                    "DataType": "String",
                    "StringValue": example_deployment_string
                }
            }
        }
    ]
}

mock_sqs_message_body = {
    "Messages": [
        {
            "MessageId": "mock-message-id",
            "ReceiptHandle": "mock-receipt-handle",
            "Body": json.dumps({
                "metadata": {
                    "DataType": "String",
                    "StringValue": example_metadata_string
                },
                "deployment": {
                    "DataType": "String",
                    "StringValue": example_deployment_string
                }
            })
        }
    ]
}

sts_get_caller_identity_response = {

    'AccessKeyId': 'test-access-key-id',
    'SecretAccessKey': 'test-secret-key',
    'SessionToken': 'test-session-token',
    'Expiration': datetime.datetime.now() + datetime.timedelta(minutes=5)

}

mock_unknown_sqs_message_type = {
    "Messages": [
        {
            "MessageId": "mock-message-id",
            "ReceiptHandle": "mock-receipt-handle",
            "Body": "mock-body",
            "MessageAttributes": {
                "metadata": {
                    "DataType": "String",
                    "StringValue": example_metadata_string
                },
                "UNKNOWN_TYPE": {
                    "DataType": "String",
                    "StringValue": "{\"foo\": \"bar\"}"
                }
            }
        }
    ]
}


def mock_asset_request(url, **params):
    return mock.Mock(status_code=200, text="""
    {
        "data": [
            {
                "id": "1",
                "type": "restaurants",
                "attributes": {
                    "created": "2020-06-15T17:08:41.000+0000",
                    "name": "Mock Restaurant",
                    "updated": "2020-06-15T17:08:41.000+0000"
                }
            }
        ]
    }
    """)


def mock_sqs_client(response):
    sqs = mock.Mock()
    sqs.receive_message = mock.Mock(return_value=response)
    return sqs


def mock_get_temporary_credential(response):
    STS_CLIENT = MagicMock()
    STS_CLIENT.assume_role = mock.Mock(return_value=response)
    return STS_CLIENT


def mock_batch_v1_beta1_api():
    mock_api = mock.Mock()
    mock_api.read_namespaced_cron_job = mock.Mock(return_value=mock_cron_job())
    return mock_api


def mock_cron_job():
    mock_job_spec = mock.Mock()
    mock_job_spec.template.spec.containers = [Mock(), Mock()]
    mock_cron_job = Mock()
    mock_cron_job.spec.job_template.spec = mock_job_spec
    return mock_cron_job


@mock.patch('eds.coreV1Api.read_namespaced_config_map', Mock())
def test_missing_restaurant_exits_unsuccessfully():
    with pytest.raises(SystemExit) as context_manager:
        eds.main()
    # Verify process ends with exit code of 1
    assert context_manager.value.code == 1


@mock.patch('eds.get_temporary_credentials',
            mock_get_temporary_credential(sts_get_caller_identity_response))
@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client({}))
def test_empty_sqs_response_exits_successfully():
    os.environ["restaurant_id"] = "1"
    with pytest.raises(SystemExit) as context_manager:
        eds.main()
    # Verify process ends with exit code of 0
    assert context_manager.value.code == 0


def mock_read_namespaced_deployment_status(k8s_name, k8s_namespace):
    # Increment the replica attributes each call to provide different outputs. Note that
    # the attribute needs to be reset on the function itself for each test that calls this.
    mock_read_namespaced_deployment_status.replicas += 1
    mock_deployment_status = mock.Mock()
    mock_deployment_status.status.ready_replicas = mock_read_namespaced_deployment_status.replicas
    mock_deployment_status.status.updated_replicas = mock_read_namespaced_deployment_status.replicas
    mock_deployment_status.spec.replicas = 2
    return mock_deployment_status


def mock_read_namespaced_config_map(k8s_config_map_name, k8s_namespace):
    mock_config_map = mock.Mock()
    mock_config_map.data = {}
    return mock_config_map


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_deployment_sqs_message))
@mock.patch('eds.appsV1Api.read_namespaced_deployment_status', mock_read_namespaced_deployment_status)
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch(create_namespaced_deployment, Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.get_temporary_credentials',
            mock_get_temporary_credential(sts_get_caller_identity_response)(sts_get_caller_identity_response))
@mock.patch('eds.sleep', Mock())
def test_deployment_message_triggers_opentest():
    mock_read_namespaced_deployment_status.replicas = 0
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch(create_namespaced_deployment) as mock_create_namespaced_deployment:
        eds.main()
    assert mock_create_namespaced_deployment.called
    assert mock_asset_service_patch_request.called
    assert mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_deployment_sqs_message))
@mock.patch('eds.appsV1Api.read_namespaced_deployment_status', mock_read_namespaced_deployment_status)
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch(create_namespaced_deployment, side_effect=client.rest.ApiException(409))
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('eds.sleep', Mock())
def test_duplicate_deployment_message_triggers_replace_resource(mock_create_namespaced_deployment):
    mock_read_namespaced_deployment_status.replicas = 0
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch('eds.appsV1Api.replace_namespaced_deployment') as mock_replace_namespaced_deployment:
        eds.main()
    assert mock_replace_namespaced_deployment.called
    assert mock_asset_service_patch_request.called
    assert mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_cronjob_sqs_message))
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('eds.sleep', Mock())
def test_cronjob_message_does_not_trigger_opentest():
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch('eds.batchV1beta1Api.create_namespaced_cron_job') as mock_create_namespaced_cronjob:
        eds.main()
    assert mock_create_namespaced_cronjob.called
    assert mock_asset_service_patch_request.called
    assert not mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_cronjob_sqs_message))
@mock.patch('eds.batchV1beta1Api.create_namespaced_cron_job', side_effect=client.rest.ApiException(409))
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('eds.sleep', Mock())
def test_duplicate_cronjob_message_triggers_replace_resource(mock_create_namespaced_cron_job):
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch('eds.batchV1beta1Api.replace_namespaced_cron_job') as mock_replace_namespaced_cron_job:
        eds.main()
    assert mock_replace_namespaced_cron_job.called
    assert mock_asset_service_patch_request.called
    assert not mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_persistentvolume_sqs_message))
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('eds.sleep', Mock())
def test_persistentvolume_message_does_not_trigger_opentest():
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch('eds.coreV1Api.create_persistent_volume') as mock_create_persistent_volume:
        eds.main()
    assert mock_create_persistent_volume.called
    assert not mock_asset_service_patch_request.called
    assert not mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_persistentvolume_sqs_message))
@mock.patch('eds.coreV1Api.create_persistent_volume', side_effect=client.rest.ApiException(409))
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('eds.sleep', Mock())
def test_duplicate_persistentvolume_message_triggers_replace_resource(mock_create_persistent_volume):
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch('eds.coreV1Api.patch_persistent_volume') as mock_patch_persistent_volume:
        eds.main()
    assert mock_patch_persistent_volume.called
    assert not mock_asset_service_patch_request.called
    assert not mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_persistentvolumeclaim_sqs_message))
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.sleep', Mock())
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
def test_persistentvolumeclaim_message_does_not_trigger_opentest():
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch(
                'eds.coreV1Api.create_namespaced_persistent_volume_claim') as mock_create_persistent_volume_claim:
        eds.main()
    assert mock_create_persistent_volume_claim.called
    assert not mock_asset_service_patch_request.called
    assert not mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_persistentvolumeclaim_sqs_message))
@mock.patch('eds.coreV1Api.create_namespaced_persistent_volume_claim', side_effect=client.rest.ApiException(409))
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.sleep', Mock())
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
def test_duplicate_persistentvolumeclaim_message_triggers_replace_resource(
        mock_create_namespaced_persistent_volume_claim):
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch(
                'eds.coreV1Api.patch_namespaced_persistent_volume_claim') as mock_patch_namespaced_persistent_volume_claim:
        eds.main()
    assert mock_patch_namespaced_persistent_volume_claim.called
    assert not mock_asset_service_patch_request.called
    assert not mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_sqs_message_body))
@mock.patch('eds.wait_for_deployment_to_complete', Mock())
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('eds.applyk8s_message', Mock())
def test_deployment_message_body_triggers_opentest():
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request:
        eds.main()
    assert mock_asset_service_patch_request.called
    assert mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_sqs_message_body))
@mock.patch('eds.wait_for_deployment_to_complete', Mock(return_value=False))
@mock.patch('eds.update_deployment_history', Mock())
@mock.patch('eds.appsV1Api.read_namespaced_deployment', Mock(return_value=True))
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.applyk8s_message', Mock())
@mock.patch('eds.get_image_version_from_deployment', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
def test_timedout_deployment_does_not_update_assets_service():
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request:
        eds.main()
    assert not mock_asset_service_patch_request.called
    assert not mock_opentest_job.called


@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_deployment_sqs_message))
@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('eds.appsV1Api.read_namespaced_deployment_status', mock_read_namespaced_deployment_status)
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.sleep', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
def test_deployment_message_existing_resource_exceptions_caught():
    mock_read_namespaced_deployment_status.replicas = 0
    os.environ["restaurant_id"] = "1"
    with mock.patch('eds.coreV1Api.create_namespace') as mock_create_namespace, \
            mock.patch(create_namespaced_deployment) as mock_create_namespaced_deployment, \
            mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch('eds.appsV1Api.replace_namespaced_deployment') as mock_replace_namespaced_deployment, \
            mock.patch(create_namespaced_deployment) as mock_create_namespaced_deployment:
        mock_create_namespace.side_effect = client.rest.ApiException()
        mock_create_namespace.side_effect.status = 409
        mock_create_namespaced_deployment.side_effect = client.rest.ApiException()
        mock_create_namespaced_deployment.side_effect.status = 409
        eds.main()
    assert mock_create_namespaced_deployment.called
    assert mock_asset_service_patch_request.called
    assert mock_replace_namespaced_deployment.called
    assert mock_opentest_job.called


@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_sqs_message_no_metadata))
@mock.patch('eds.wait_for_deployment_to_complete', Mock())
@mock.patch('eds.update_deployment_history', Mock())
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.batchV1Api.create_namespaced_job', Mock())
@mock.patch('eds.applyk8s_message', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
def test_deployment_message_no_metadata_triggers_opentest():
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request:
        eds.main()
    assert mock_asset_service_patch_request.called
    assert mock_opentest_job.called


def test_unkown_aws_account_number_exits_unsuccessfully():
    os.environ["AWS_ACCOUNT"] = "123456789012"
    with pytest.raises(SystemExit) as context_manager:
        eds.main()
    # Verify process ends with exit code of 1
    assert context_manager.value.code == 1
    # Clean up the environment variable so it doesn't affect subsequent tests
    del os.environ["AWS_ACCOUNT"]


@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_unknown_sqs_message_type))
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.sleep', Mock())
def test_unknown_create_resource_type():
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch('eds.logger.error') as mock_logger_error, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request:
        eds.main()
    assert mock_logger_error.called
    assert not mock_asset_service_patch_request.called
    assert not mock_opentest_job.called


@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_deployment_sqs_message))
@mock.patch('eds.appsV1Api.read_namespaced_deployment_status', mock_read_namespaced_deployment_status)
@mock.patch('eds.batchV1beta1Api', mock_batch_v1_beta1_api())
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.sleep', Mock())
def test_missing_opentest_exception_caught():
    mock_read_namespaced_deployment_status.replicas = 0
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request, \
            mock.patch('eds.batchV1Api.create_namespaced_job') as mock_create_namespaced_job, \
            mock.patch(create_namespaced_deployment) as mock_create_namespaced_deployment:
        mock_opentest_job.side_effect = client.rest.ApiException()
        mock_opentest_job.side_effect.status = 404
        eds.main()
    assert mock_create_namespaced_deployment.called
    assert mock_asset_service_patch_request.called
    assert mock_opentest_job.called
    assert not mock_create_namespaced_job.called


@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('requests.get', mock_asset_request)
@mock.patch('eds.sqs', mock_sqs_client(mock_certificatedelivery_sqs_message))
@mock.patch('eds.coreV1Api.create_namespace', Mock())
@mock.patch('eds.coreV1Api.create_namespaced_config_map', Mock())
@mock.patch('eds.coreV1Api.read_namespaced_config_map', mock_read_namespaced_config_map)
@mock.patch('eds.sleep', Mock())
def test_certificatedelivery_message_patches_assets_service():
    os.environ["restaurant_id"] = "1"
    with mock.patch(v1_job) as mock_opentest_job, \
            mock.patch('eds.coreV1Api.create_namespaced_secret') as mock_create_namespaced_secret, \
            mock.patch(patch_request_str) as mock_asset_service_patch_request:
        eds.main()
    assert mock_create_namespaced_secret.called
    assert mock_asset_service_patch_request.called
    assert not mock_opentest_job.called


@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('eds.configure', Mock())
@mock.patch('eds.get_sqs_message', Mock())
def test_loading_kube_config():
    del os.environ["SKIP_LOADING_KUBE_CONFIG"]
    with mock.patch('kubernetes.config.load_kube_config') as mock_load_kube_config:
        importlib.reload(eds)
    assert mock_load_kube_config.called


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


@mock.patch('eds.get_temporary_credentials', mock_get_temporary_credential(sts_get_caller_identity_response)())
@mock.patch('eds.configure', Mock())
@mock.patch('eds.get_sqs_message', Mock())
@mockenv(AWS_ACCOUNT="688810906228")
def test_loading_kube_config_1():
    os.environ["AWS_ACCOUNT"] = "688810906228"
    with mock.patch('kubernetes.config.load_kube_config') as mock_load_kube_config, \
            mock.patch.dict(os.environ, {}, clear=True), \
            mock.patch.dict(os.environ, {'AWS_ACCOUNT': '688810906228'}):
        eds.configure()
    eds.get_queue_prefix_and_assets_url("688810906228")
    eds.get_queue_prefix_and_assets_url("524430043955")
    eds.get_queue_prefix_and_assets_url("593265675765")
    eds.get_namespace("iot-name")
    assert not mock_load_kube_config.called
