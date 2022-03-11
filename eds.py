import os
import boto3
import json
import requests
import time
import sys
import logging2
from aws_requests_auth.aws_auth import AWSRequestsAuth
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
from uuid import uuid4
from time import sleep
from kubernetes import client, config
from io import StringIO

BRED_NAMESPACE = "bred"
CLUSTER_NAME_CONFIG_MAP = "cluster-name"

logger = logging2.Logger("")
sqs = boto3.client("sqs")
logger.info("Checking for cluster config")
if not os.environ.get("SKIP_LOADING_KUBE_CONFIG"):
    try:
        config.load_incluster_config()
        logger.info("Cluster config loaded successfully")
    except config.config_exception.ConfigException:
        logger.info("Failed to load cluster config, defaulting to kube config")
        config.load_kube_config()
coreV1Api = client.CoreV1Api()
customV1Api = client.CustomObjectsApi()
appsV1Api = client.AppsV1Api()
extensionsV1beta1Api = client.ExtensionsV1beta1Api()
batchV1beta1Api = client.BatchV1beta1Api()
batchV1Api = client.BatchV1Api()
rbacAuthorizationV1Api = client.RbacAuthorizationV1Api()
https_prefix = "https://"
application_type = "application/vnd.api+json"

config = {}



def get_sqs_message():
    # asset_restaurant_id_response = req_url
    asset_restaurant_id_response = json.loads(requests.get(
        f"https://{config['assetsUrl']}/restaurant_assets/restaurants?filter[name]={config['restaurantId']}",
        auth=config["awsAuth"]))
    # asset_restaurant_id_response = json.loads(asset_restaurant_id_response.text)
    asset_restaurant_id = asset_restaurant_id_response["data"][0]["id"]

    queue_url = f"{config['queueBaseUrl']}{config['queuePrefix']}{asset_restaurant_id}{config['queueSuffix']}"
    logger.info(f"queue_url: {queue_url}")
    logger.info("Entering to process response") 
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=["All"],
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
        VisibilityTimeout=600,  # VisibilityTimeout should be higher than the deployment timeout (300s)
        WaitTimeSeconds=5,
    )
    logger.info("After completition of response process")
    message_id, receipt_handle, messages = get_message_data_from_response(response)
    k8s_namespace, component_name, k8s_name = get_k8s_data_from_messages(messages)
    logger.info(f"Received message with id of {message_id}")

    logger.info(f"Processed message {messages}")

    primary_resource_kind = loop_through_messages(
        messages,
        asset_restaurant_id,
        config["assetsUrl"],
        component_name,
        k8s_name,
        k8s_namespace,
    )
    delete_sqs_message(queue_url, receipt_handle, message_id)

    if primary_resource_kind in ["statefulset", "daemonset"]:
        trigger_open_test(k8s_name)


def get_queue_prefix_and_assets_url(aws_account):
    # Adding a condition to check the existing Env
    if aws_account == "283388140277":
        queue_prefix = os.getenv("queue_prefix", "US-EAST-DEV-BRE-ARCH-BRE-")
        assets_url = os.environ.get("assets_url", "asset.api.dev.bre.mcd.com")
        iot_get_certificate_url = os.environ.get("iot_get_certificate_url",
                                                 "device-api.iot.dev.bre.mcd.com")
        iot_role_arn = os.environ.get("iot_role_arn",
                                      "arn:aws:iam::385911300465:role/US-EAST-DEV-US-BRE-ARCH-IoT_API_Role")

    elif aws_account == "524430043955":
        queue_prefix = os.getenv("queue_prefix", "US-EAST-PROD-BRE-ARCH-BRE-")
        assets_url = os.environ.get("assets_url", "asset.api.prod.bre.mcd.com")
        iot_get_certificate_url = os.environ.get("iot_get_certificate_url",
                                                 "device-api.iot.prod.bre.mcd.com")
        iot_role_arn = os.environ.get("iot_role_arn",
                                      "arn:aws:iam::687478879033:role/US-EAST-DEV-US-BRE-ARCH-IoT_API_Role")

    elif aws_account == "593265675765":
        queue_prefix = os.getenv("queue_prefix", "US-EAST-INT-BRE-ARCH-BRE-")
        assets_url = os.environ.get("assets_url", "asset.api.int.bre.mcd.com")
        iot_get_certificate_url = os.environ.get("iot_get_certificate_url",
                                                 "device-api.iot.int.bre.mcd.com")
        iot_role_arn = os.environ.get("iot_role_arn",
                                      "arn:aws:iam::155065244512:role/US-EAST-INT-US-BRE-ARCH-IoT_API_Role")
    elif aws_account == "688810906228":
        queue_prefix = os.getenv("queue_prefix", "US-EAST-STG-BRE-ARCH-BRE-")
        assets_url = os.environ.get("assets_url", "asset.api.stg.bre.mcd.com")
        iot_get_certificate_url = os.environ.get("iot_get_certificate_url",
                                                 "device-api.iot.int.bre.mcd.com")
        iot_role_arn = os.environ.get("iot_role_arn",
                                      "arn:aws:iam::155065244512:role/US-EAST-INT-US-BRE-ARCH-IoT_API_Role")

    else:
        logger.error("{} AwsAccount is currently not supported ".format(aws_account))
        sys.exit(1)
    return queue_prefix, assets_url, iot_get_certificate_url, iot_role_arn


def get_message_data_from_response(response):
    messages = {}
    message_payload = {}
    logger.info("Message Response: " + str(response))
    try:
        message = response["Messages"][0]
        message_id = message["MessageId"]
        receipt_handle = message["ReceiptHandle"]
        # try to get the payload in the message body, if not, use message attributes.
        if is_valid_json(message["Body"]):
           # message_payload = message["Body"]
           # messages = json.load(StringIO(message_payload))
            messages = json.load(StringIO(message["Body"]))
            
        else:
           # message_payload = message["MessageAttributes"]
           # messages = json.loads(json.dumps(message_payload))
            messages = json.loads(json.dumps(message["MessageAttributes"]))
    except KeyError:
        logger.info("No messages in queue")
        exit(0)
    return message_id, receipt_handle, messages


def get_k8s_data_from_messages(messages):
    # print(type(messages["metadata"]["StringValue"]["metadata"]))
    try:
        if isinstance(messages["metadata"]["StringValue"], dict):
            message_metadata = messages["metadata"]["StringValue"]["metadata"]
        else:
            message_metadata = json.loads(messages["metadata"]["StringValue"])["metadata"]
        k8s_namespace = message_metadata["namespace"]
        logger.info(
            f"Namespace retrieved from the deployment manifest is {k8s_namespace}"
        )
        component_name = message_metadata["pod"]
        logger.info(
            f"Component name retrieved from the deployment manifest is {component_name}"
        )
        k8s_name = component_name
    except KeyError:
        k8s_namespace = ""
        k8s_name = ""
        component_name = ""
        logger.info("No metadata attribute")
    return k8s_namespace, component_name, k8s_name


def get_namespace(name):
    namespace = name.split("-")
    return namespace[0]

iot_url_link = f"https://" + config["iotGetCertificateUrl"] + "/on-boarding/certificate"

def try_posting_certificate_notification(messages):
    if isinstance(messages["certificate"]["StringValue"], dict):
        certificate_notification = messages["certificate"]["StringValue"]
    else:
        certificate_notification = json.loads(messages["certificate"]["StringValue"])
    # From the above payload, we only need ["attributes"]["deviceID"].
    logger.info(
        "Certificate Notification has been triggered. Message contents are {}".format(
            certificate_notification
        )
    )
    logger.info("group Id{}".format(certificate_notification['groupId']))
    onboard_json = {
        'deviceId': certificate_notification['deviceId'],
        'restaurantId': certificate_notification['restaurantId'],
        'pod': certificate_notification['componentName'],
        'namespace': get_namespace(certificate_notification['componentName']),
        'componentId': certificate_notification['componentId'],
        'rotation': certificate_notification['rotation'],
        'groupId': certificate_notification['groupId']
    }

    if certificate_notification['rotation']:
        onboard_json['componentConfigId'] = certificate_notification['componentConfigId']

    iot_url = iot_url_link
    logger.info("iot URL{}".format(iot_url))
    # Send certificate notification POST request to IoT Edge Certificate Generator.
    try:
        certificate_notification_request = requests.post(
            iot_url,
            json=onboard_json,
            auth=config["awsAuthIoT"]
        )
        logger.info("response {}".format(certificate_notification_request.text))
    except requests.exceptions.RequestException as err:
        logger.error("Unable to notify the IoT Get Certificate Handler.")
        logger.exception(err)


def check_for_previous_deployment(
        k8s_name,
        primary_resource_kind,
        k8s_namespace,
        previous_deployment,
        k8s_pv_name,
        k8s_pvc_name,
        asset_restaurant_id,
):
    if previous_deployment:
        version_to_roll_back_to = get_image_version_from_deployment(previous_deployment)
        logger.info(
            f"Rolling back deployment '{k8s_name}' to version '{version_to_roll_back_to}'..."
        )
        applyk8s_message(
            k8s_name,
            primary_resource_kind,
            k8s_namespace,
            previous_deployment,
            k8s_pv_name,
            k8s_pvc_name,
            asset_restaurant_id,
        )
        logger.info(
            f"Successfully rolled back deployment '{k8s_name}' to version '{version_to_roll_back_to}'"
        )
    else:
        logger.info(
            f"Cannot roll-back deployment '{k8s_name}', as this is a first-time deployment."
        )


def is_valid_json(payload):
    try:
        json_object = json.loads(payload)
        logger.info(
            f"valid json '{json_object}'"
        )

    except ValueError as e:
        logger.exception(
            f"exception details '{e}'"
        )
        return False
    return True


def get_image_version_from_deployment(deployment):
    if not isinstance(deployment, client.V1Deployment):
        # if deployment isn't already a V1Deployment object, then assume it's a dictionary and parse
        return deployment["spec"]["template"]["spec"]["containers"][0]["image"].split(
            ":"
        )[1]
    else:
        return deployment.spec.template.spec.containers[0].image.split(":")[1]


def get_image_version_from_job(job):
    return job["spec"]["template"]["spec"]["containers"][0]["image"].split(":")[1]


def get_image_version_from_cron_job(cronjob):
    return cronjob["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0][
        "image"
    ].split(":")[1]


def applyk8s_message(
        k8s_name, k8s_kind, k8s_namespace, k8s_message, k8s_pv_name, k8s_pvc_name, asset_restaurant_id
):
    try:
        coreV1Api.create_namespace(
            client.V1Namespace(metadata=client.V1ObjectMeta(name=k8s_namespace))
        )

        cluster_name_info = coreV1Api.read_namespaced_config_map(
            CLUSTER_NAME_CONFIG_MAP, BRED_NAMESPACE
        )
        config_map_json = json.loads(
            json.dumps(
                {
                    "apiVersion": "v1",
                    "data": cluster_name_info.data,
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": CLUSTER_NAME_CONFIG_MAP,
                        "namespace": k8s_namespace,
                    },
                },
                sort_keys=False,
                indent=None,
            )
        )

        coreV1Api.create_namespaced_config_map(
            k8s_namespace, config_map_json, pretty="pretty"
        )

    except client.rest.ApiException as e:
        if e.status == 409:
            logger.debug("ns/{} already exists... skipping!".format(k8s_namespace))
    else:
        logger.info("created ns/{}".format(k8s_namespace))
        # copying imagepullsecrets to new NS took up to 13 seconds to apply
        sleep(20)
    try:
        create_resource(k8s_kind, k8s_namespace, k8s_message)
    except client.rest.ApiException as e:
        if e.status == 409:
            replace_resource(
                k8s_name, k8s_kind, k8s_namespace, k8s_message, k8s_pv_name, k8s_pvc_name
            )
        else:
            logger.error(
                f"applying {k8s_kind}/{k8s_name} in ns/{k8s_namespace} failed! Error code: {e.status}, Reason: {e.reason}."
            )
            logger.error(f"Headers: {e.headers}.")
            logger.error(f"Body: {e.body}.")
    else:
        logger.info("created {}/{} in ns/{}".format(k8s_kind, k8s_name, k8s_namespace))


def create_resource(k8s_kind, k8s_namespace, k8s_message):
    # initial create resource
    if k8s_kind == "deployment":
        appsV1Api.create_namespaced_deployment(
            k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "service":
        coreV1Api.create_namespaced_service(k8s_namespace, k8s_message, pretty="pretty")
    elif k8s_kind == "ingress":
        extensionsV1beta1Api.create_namespaced_ingress(
            k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "cronjob":
        batchV1beta1Api.create_namespaced_cron_job(
            k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "job":
        batchV1Api.create_namespaced_job(k8s_namespace, k8s_message, pretty="pretty")
    elif k8s_kind == "persistentvolume":
        coreV1Api.create_persistent_volume(k8s_message, pretty="pretty")
    elif k8s_kind == "persistentvolumeclaim":
        coreV1Api.create_namespaced_persistent_volume_claim(
            k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "role":
        rbacAuthorizationV1Api.create_namespaced_role(
            k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "rolebinding":
        rbacAuthorizationV1Api.create_namespaced_role_binding(
            k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "serviceaccount":
        coreV1Api.create_namespaced_service_account(
            k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind in ("secret", "certificatedelivery"):
        coreV1Api.create_namespaced_secret(k8s_namespace, k8s_message, pretty="pretty")
    elif k8s_kind == "configmap":
        coreV1Api.create_namespaced_config_map(
            k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "ingressrouteudp":
        api_group_version = k8s_message["api_version"]
        api_group = api_group_version.split("/")[0]
        api_version = api_group_version.split("/")[1]
        plurals = f"{k8s_kind.lower()}s"
        logger.info(
            f"creating ingressrouteudp with api_group={api_group}, api_version={api_version}, k8sNamespace={k8s_namespace}, plurals={plurals}"
        )
        customV1Api.create_namespaced_custom_object(
            api_group, api_version, k8s_namespace, plurals, k8s_message, pretty="pretty"
        )
    else:
        logger.error(f"Unsupported resource type {k8s_kind}")


def replace_resource(k8s_name, k8s_kind, k8s_namespace, k8s_message, k8s_pv_name, k8s_pvc_name):
    # If resource already exists
    logger.info(
        "{}/{} in ns/{} already exists... replacing".format(
            k8s_kind, k8s_name, k8s_namespace
        )
    )
    if k8s_kind == "deployment":
        appsV1Api.replace_namespaced_deployment(
            k8s_name, k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "service":
        conditionally_replace_namespaced_service(k8s_name, k8s_namespace, k8s_message)
    elif k8s_kind == "ingress":
        extensionsV1beta1Api.replace_namespaced_ingress(
            k8s_name, k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "cronjob":
        batchV1beta1Api.replace_namespaced_cron_job(
            k8s_name, k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "job":
        logger.info(
            "deleting existing {}/{} in ns/{}...".format(k8s_kind, k8s_name, k8s_namespace)
        )
        batchV1Api.delete_namespaced_job(k8s_name, k8s_namespace, pretty="pretty")
        sleep(10)
        logger.info(
            "recreating {}/{} in ns/{}...".format(k8s_kind, k8s_name, k8s_namespace)
        )
        batchV1Api.create_namespaced_job(k8s_namespace, k8s_message, pretty="pretty")
    elif k8s_kind == "persistentvolumeclaim":
        coreV1Api.patch_namespaced_persistent_volume_claim(
            k8s_pvc_name, k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "persistentvolume":
        coreV1Api.patch_persistent_volume(k8s_pv_name, k8s_message, pretty="pretty")
    elif k8s_kind == "role":
        rbacAuthorizationV1Api.replace_namespaced_role(
            k8s_name, k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "rolebinding":
        rbacAuthorizationV1Api.replace_namespaced_role_binding(
            k8s_name, k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "serviceaccount":
        coreV1Api.replace_namespaced_service_account(
            k8s_name, k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind in ("secret", "certificatedelivery"):
        coreV1Api.replace_namespaced_secret(
            k8s_name, k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "configmap":
        coreV1Api.replace_namespaced_config_map(
            k8s_name, k8s_namespace, k8s_message, pretty="pretty"
        )
    elif k8s_kind == "ingressrouteudp":
        api_group_version = k8s_message["api_version"]
        api_group = api_group_version.split("/")[0]
        api_version = api_group_version.split("/")[1]
        plurals = f"{k8s_kind.lower()}s"
        logger.info(
            f"replacing ingressrouteudp with api_group={api_group}, api_version={api_version}, k8sNamespace={k8s_namespace}, plurals={plurals}"
        )
        ingressrouteudp_nested(api_group, api_version, k8s_kind, k8s_message, k8s_name, k8s_namespace, plurals)
    else:
        logger.error(f"Unsupported resource type {k8s_kind}")


def ingressrouteudp_nested(api_group, api_version, k8s_kind, k8s_message, k8s_name, k8s_namespace, plurals):
    try:
        logger.info(
            "patching {}/{} in ns/{}...".format(k8s_kind, k8s_name, k8s_namespace)
        )
        customV1Api.patch_namespaced_custom_object(
            api_group, api_version, k8s_namespace, plurals, k8s_name, k8s_message
        )
    except client.rest.ApiException as e:
        logger.info(
            "patch failed. replacing {}/{} in ns/{}...".format(
                k8s_kind, k8s_name, k8s_namespace
            )
        )
        logger.exception(
            f"exception details '{e}'"
        )
        customV1Api.replace_namespaced_custom_object(
            api_group, api_version, k8s_namespace, plurals, k8s_name, k8s_message
        )


def conditionally_replace_namespaced_service(k8s_name, k8s_namespace, k8s_message):
    if "resourceVersion" in k8s_message and not k8s_message["resourceVersion"]:
        k8s_message.pop("resourceVersion")
        coreV1Api.replace_namespaced_service(
            k8s_name, k8s_namespace, k8s_message, pretty="pretty"
        )


def trigger_open_test(k8s_name):
    logger.info("Triggering OpenTest integration test")
    try:
        open_test_cron_job = batchV1beta1Api.read_namespaced_cron_job(
            "qe-opentest-integration", "qe"
        )
        open_test_job_spec = open_test_cron_job.spec.job_template.spec
        open_test_job_spec.template.spec.containers[1].env.append(
            client.V1EnvVar(name="DEPLOYMENT_NAME", value=k8s_name)
        )
        # max length is 63 chars
        job_name = f"opentest-{k8s_name}-{uuid4().hex[0:16]}"[0:63]

        new_open_test_job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.models.V1ObjectMeta(name=job_name),
            spec=open_test_job_spec,
        )

        batchV1Api.create_namespaced_job("qe", new_open_test_job, pretty="pretty")
        logger.info("OpenTest integration test successfully launched")
    except client.rest.ApiException as e:
        if e.status == 404:
            logger.info(
                f"OpenTest integration does not exist on this cluster. Skipping..."
            )
        else:
            logger.error(
                f"Could not launch OpenTest: Error code: {e.status}, Reason: {e.reason}. Skipping..."
            )


def delete_sqs_message(sqs_queue, receipt_handle, message_id):
    # Delete received message from queue
    logger.info(f"Deleting sqs message with id of {message_id}")
    sqs.delete_message(QueueUrl=sqs_queue, ReceiptHandle=receipt_handle)


def wait_for_deployment_to_complete(
        k8s_name, k8s_namespace, primary_resource_kind, timeout=300
):
    start = time.time()
    logger.info(f"Waiting for deployment '{k8s_name}' to succeed...")
    logger.info(f"Resource kind is : {primary_resource_kind}")
    while time.time() - start < timeout:
        sleep(2)
        try:
            # Wait for deployment only
            if primary_resource_kind != "deployment":
                return False
            deployment = appsV1Api.read_namespaced_deployment_status(
                k8s_name, k8s_namespace
            )
        except client.rest.ApiException as e:
            # Handles 404 or 401 or 400 error
            logger.error(
                f"applying {k8s_name} in ns/{k8s_namespace} failed! Error code: {e.status}, Reason: {e.reason}."
            )
            logger.error(f"Headers: {e.headers}.")
            logger.error(f"Body: {e.body}.")
            return False
        status = deployment.status
        if (
                status.ready_replicas == deployment.spec.replicas
                and status.updated_replicas == deployment.spec.replicas
        ):
            # if ready_replicas and updated_replicas are equal to the deployment's expected replicas, success!
            logger.info(f"Successfully deployed '{k8s_name}'!")
            return deployment
        else:
            elapsed_seconds = time.time() - start
            logger.info(
                f"Updated replicas: {status.updated_replicas} of {deployment.spec.replicas}... ({int(elapsed_seconds)}/{timeout}s)"
            )

    logger.error(f"Deployment did not complete after {timeout} seconds")
    return False


def get_component_id(asset_restaurant_id, assets_url, k8s_name):
    assets_url_params = {
        "filter[name]": k8s_name,
        "filter[restaurant.id]": asset_restaurant_id,
        "fields[components]": "id",
    }
    
    # assets_url_response = requests.get(
    #   f"{https_prefix}{assets_url}/restaurant_assets/components",
    #    params=assets_url_params,
    #    auth=config["awsAuth"],
    #)
    assets_url_response_json = json.loads(requests.get(
        f"{https_prefix}{assets_url}/restaurant_assets/components",
        params=assets_url_params,
        auth=config["awsAuth"],
    ))
    # assets_url_response_json = json.loads(assets_url_response.text)
    component_id = assets_url_response_json["data"][0]["id"]

    return component_id


# get deployment history id for the component
def get_deployment_details(asset_restaurant_id, assets_url, group_id):
    history_params = {
        "filter[restaurant.id]": asset_restaurant_id,
        "filter[deployment_group.id]": group_id,
    }
    history_response_json = json.loads(requests.get(
        f"{https_prefix}{assets_url}/restaurant_assets/deployment_history",
        params=history_params,
        auth=config["awsAuth"],
    ))
    # history_response = requests.get(
    #    f"{https_prefix}{assets_url}/restaurant_assets/deployment_history",
    #    params=history_params,
    #    auth=config["awsAuth"],
    #)
    # history_response_json = json.loads(history_response.text)
    history_id = history_response_json["data"][0]["id"]
    return history_id


# get deployment group id from the message metadata
def get_deployment_group_id(messages):
    msg_metadata = json.loads(messages["metadata"]["StringValue"])["metadata"]

    if "deploymentGroupId" in msg_metadata:
        deployment_id = msg_metadata["deploymentGroupId"]
    else:
        deployment_id = "0"
    logger.info(
        f"Deployment group id name from the deployment manifest is {deployment_id}"
    )
    return deployment_id


def update_asset_service(
        component_name,
        k8s_name,
        deployment_version,
        assets_url,
        asset_restaurant_id,
        has_multiple_components,
):
    if "restaurant-assets" in k8s_name:
        # after we deploy asset svc, we need to let it restart before running queries
        sleep(60)

    if has_multiple_components:
        component_id = get_component_id(asset_restaurant_id, assets_url, component_name)
    else:
        component_id = get_component_id(asset_restaurant_id, assets_url, k8s_name)

    response = requests.get(
        f"{https_prefix}{assets_url}/restaurant_assets/components/{component_id}",
        auth=config["awsAuth"],
    )

    payload = {
        "data": {
            "type": "components",
            "attributes": {"reportedVersion": deployment_version},
        }
    }
    headers = {"content-type": application_type}

    if response.status_code != 200:
        logger.error("API Endpoint is currently not responding")
    else:
        logger.info(
            "Attempting to patch component name "
            + k8s_name
            + " with version: "
            + deployment_version
        )
        requests.patch(
            f"{https_prefix}{assets_url}/restaurant_assets/components/{component_id}",
            data=json.dumps(payload),
            headers=headers,
            auth=config["awsAuth"],
        )

        logger.info("Patching Assets API successful!")


# code to update deployment history
def update_deployment_history(asset_restaurant_id, assets_url, messages, status):
    groupid = get_deployment_group_id(messages)

    if groupid == "0":
        logger.info("No need to update deployment history")
    else:
        # get deployment history details
        history_id = get_deployment_details(asset_restaurant_id, assets_url, groupid)
        logger.info("Deployment history to be updated: " + history_id)

        payload = {
            "data": {"type": "deployment_history", "attributes": {"status": status}}
        }
        headers = {"content-type": application_type}

        api_response = requests.patch(
            f"{https_prefix}{assets_url}/restaurant_assets/deployment_history/{history_id}",
            data=json.dumps(payload),
            headers=headers,
            auth=config["awsAuth"],
        )
        logger.info("Patching Deployment Status successful!")

        if api_response.status_code == 200:
            logger.error(
                "Deployment status updated for deployment history id "
                + history_id
                + " with status "
                + status
            )
        else:
            logger.info(
                "Deployment status updation failed for history id: " + history_id
            )


def confirm_certificate_delivery(k8s_name, assets_url, asset_restaurant_id):
    component_id = get_component_id(asset_restaurant_id, assets_url, k8s_name)

    assets_url_params = {
        "filter[propertyName]": "OnboardingStatus",
        "filter[component.id]": component_id,
        "fields[component_props]": "id",
    }
    property_response_json = json.loads(requests.get(
        f"{https_prefix}{assets_url}/restaurant_assets/component_props",
        params=assets_url_params,
        auth=config["awsAuth"],
    ))
    # property_response = requests.get(
    #    f"{https_prefix}{assets_url}/restaurant_assets/component_props",
    #    params=assets_url_params,
    #   auth=config["awsAuth"],
    #)
    # property_response_json = json.loads(property_response.text)
    property_id = property_response_json["data"][0]["id"]

    payload = {
        "data": {"type": "component_props", "attributes": {"propertyValue": "Success"}}
    }
    headers = {"content-type": application_type}

    if property_response_json.status_code != 200:
        logger.error("API Endpoint is currently not responding")
    else:
        logger.info(
            "Attempting to patch OnboardingStatus to Completed for component name "
            + k8s_name
        )
        requests.patch(
            f"{https_prefix}{assets_url}/restaurant_assets/component_props/{property_id}",
            data=json.dumps(payload),
            headers=headers,
            auth=config["awsAuth"],
        )
        logger.info("Patching Assets API successful!")


def get_temporary_credentials(access_key, secret_key, iot_role_arn):
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    sts_client = session.client('sts')

    assumed_role_object = sts_client.assume_role(
        RoleArn=iot_role_arn,
        RoleSessionName="AssumeRoleSession1"
    )

    credentials = assumed_role_object['Credentials']
    return credentials


def configure():
    if "restaurant_id" in os.environ:
        restaurant_id = str(os.getenv("restaurant_id"))
        logger.info(f"Restaurant id: {restaurant_id}")
    else:
        logger.error("'restaurant_id' env variable does not exist")
        sys.exit(1)

    # Setting up Env variable for AwsAccount , Region
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    aws_account = os.getenv("AWS_ACCOUNT", "283388140277")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    queue_base_url = f"https://sqs.{aws_region}.amazonaws.com/{aws_account}/"
    logger.info(f"Base SQS Queue URL: {queue_base_url}")
    queue_suffix = "-SQS-DEPLOYMENT.fifo"

    queue_prefix, assets_url, iot_get_certificate_url, iot_role_arn = get_queue_prefix_and_assets_url(aws_account)

    aws_auth = BotoAWSRequestsAuth(
        aws_host=assets_url, aws_region=aws_region, aws_service="execute-api"
    )
    credentials = get_temporary_credentials(aws_access_key, aws_secret_key, iot_role_arn)

    aws_auth_iot = AWSRequestsAuth(credentials["AccessKeyId"], credentials["SecretAccessKey"],
                                   iot_get_certificate_url, 'us-east-1', 'execute-api',
                                   credentials["SessionToken"])
    assets_url_response_json = json.loads( requests.get(
        f"https://{assets_url}/restaurant_assets/restaurants?filter[name]={restaurant_id}",
        auth=aws_auth,
    ))
    
    # asset_restaurant_id_response = requests.get(
    #    f"https://{assets_url}/restaurant_assets/restaurants?filter[name]={restaurant_id}",
    #    auth=aws_auth,
    #)
    # assets_url_response_json = json.loads(asset_restaurant_id_response.text)
    logger.info(f"Restaurant details : {assets_url_response_json}")
    
    asset_restaurant_id = assets_url_response_json["data"][0]["id"]

    config["restaurantId"] = restaurant_id
    config["awsAccount"] = aws_account
    config["awsRegion"] = aws_region
    config["queueBaseUrl"] = queue_base_url
    config["queueSuffix"] = queue_suffix
    config["assetsUrl"] = assets_url
    config["queuePrefix"] = queue_prefix
    config["awsAuth"] = aws_auth
    config["assetRestaurantId"] = asset_restaurant_id
    config["iotGetCertificateUrl"] = iot_get_certificate_url
    config["awsAuthIoT"] = aws_auth_iot


def loop_through_messages(
        messages, asset_restaurant_id, assets_url, component_name, k8s_name, k8s_namespace
):
    # set to None by default so we can recognize cases where a SQS message does not change a primary resource
    primary_resource_kind = None
    k8s_pv_name = None
    k8s_pvc_name = None
    k8s_image_version = None
    previous_deployment = None
    # svcs,svcs-restaurant-assets,svcs-restaurant-assets
    for attribute_name, attribute_value in messages.items():
        k8s_kind = attribute_name.lower()
        has_multiple_components, k8s_kind, k8s_message, k8s_name = loop_through_messages_1(attribute_value, k8s_kind,
                                                                                           k8s_name)
        if k8s_kind == "cronjob":
            primary_resource_kind = k8s_kind
            k8s_image_version = get_image_version_from_cron_job(k8s_message)
        elif k8s_kind == "secret" or k8s_kind == "configmap":
            k8s_name = k8s_message["metadata"]["name"]
            k8s_namespace = k8s_name.split("-")[0]
        elif k8s_kind == "certificate":
            primary_resource_kind = k8s_kind
            try_posting_certificate_notification(messages)
        elif k8s_kind == "certificatedelivery":
            primary_resource_kind = k8s_kind
            k8s_name = k8s_message["metadata"]["name"]
            k8s_namespace = k8s_name.split("-")[0]
        elif k8s_kind == "firmware":
            primary_resource_kind = loop_through_messages_2(k8s_kind, k8s_message)
        elif k8s_kind == "persistentvolume":
            k8s_pv = k8s_message
            k8s_pv_name = k8s_pv["metadata"]["name"]
        elif k8s_kind == "persistentvolumeclaim":
            k8s_pvc = k8s_message
            k8s_pvc_name = k8s_pvc["metadata"]["name"]
        elif k8s_kind == "ingress":
            primary_resource_kind = k8s_kind
        elif k8s_kind == "deployment":
            k8s_image_version, previous_deployment, primary_resource_kind = loop_through_messages_3(k8s_kind,
                                                                                                    k8s_message, k8s_name,
                                                                                                    k8s_namespace)

        elif k8s_kind == "job":
            primary_resource_kind = k8s_kind
            k8s_image_version = get_image_version_from_job(k8s_message)

        # BTP-231:
        # this k8s kind "onboarddevice" deals with onboarding/action of iot(eg: greengrass) or non-iot(eg: axis cam) devices
        # takes onboarding payload from onboarding lambda through SQS queue and passes them to iot-thing-configurator
        elif k8s_kind == "onboarddevice":
            primary_resource_kind = loop_through_messages_4(assets_url, k8s_kind, k8s_message)

        if k8s_kind not in ["metadata", "certificate", "firmware", "onboarddevice"]:
            applyk8s_message(
                k8s_name,
                k8s_kind,
                k8s_namespace,
                k8s_message,
                k8s_pv_name,
                k8s_pvc_name,
                asset_restaurant_id,
            )

        process_primary_resource_kind(
            primary_resource_kind,
            has_multiple_components,
            component_name,
            k8s_name,
            k8s_namespace,
            k8s_image_version,
            assets_url,
            asset_restaurant_id,
            previous_deployment,
            k8s_pv_name,
            k8s_pvc_name,
            messages,
        )
    return primary_resource_kind

def loop_through_messages_4(assets_url, k8s_kind, k8s_message):
    component_id = k8s_message['deviceDetailsList'][0]['componentId']
    logger.info(
        "Component Id of the device to be onboarded = {}".format(component_id)
    )
    payload = {
        "data": {
            "type": "component_audit_history",
            "attributes": {
                "srcOfAction": "EDS",
                "action": "Message received on Edge",
                "description": "EDS received the message from the restaurant queue"
            },
            "relationships": {
                "component": {
                    "data": {
                        "type": "components",
                        "id": component_id
                    }
                }
            }
        }
    }
    headers = {"content-type": application_type}
    audit_history_response = requests.post(
        f"{https_prefix}{assets_url}/restaurant_assets/component_audit_history",
        data=json.dumps(payload),
        headers=headers,
        auth=config["awsAuth"],
    )
    logger.info(
        "Added record to component audit history table. Response = {}".format(audit_history_response.status_code)
    )
    primary_resource_kind = k8s_kind
    itc_endpoint = "http://iot-thing-configurator.iot.svc.cluster.local:8088/configurator_action"  # iot-thing-configurator endpoint
    logger.info(
        "Preparing to trigger iot-thing-configurator, Request payload= {}, Endpoint= {}".format(k8s_message,
                                                                                                itc_endpoint)
    )
    try:
        itc_response = requests.post(
            itc_endpoint,
            json=k8s_message,  # K8s message containing onboarding/action payload
        )

        logger.info(
            "iot-thing-configurator triggered, Response code = {}, Response = {}".format(
                itc_response.status_code,
                itc_response)
        )

    except requests.exceptions.RequestException as err:
        logger.error(
            "Unable to trigger iot-thing-configurator: "
        )
        logger.exception(err)
    return primary_resource_kind


def loop_through_messages_3(k8s_kind, k8s_message, k8s_name, k8s_namespace):
    primary_resource_kind = k8s_kind
    k8s_image_version = get_image_version_from_deployment(k8s_message)
    #
    logger.info(f"Found k8s image version : {k8s_image_version}")
    # save current deployment before applying new one, so we can easily roll-back to it if needed
    try:
        previous_deployment = appsV1Api.read_namespaced_deployment(
            k8s_name, k8s_namespace, export=True
        )
    except (ValueError, Exception):
        previous_deployment = None
        logger.error(
            f"Did not find existing deployment with name '{k8s_name}': has not been deployed before"
        )
    return k8s_image_version, previous_deployment, primary_resource_kind


def loop_through_messages_2(k8s_kind, k8s_message):
    primary_resource_kind = k8s_kind
    logger.info(
        "Firmware Notification has been triggered. Message contents are {}".format(
            k8s_message
        )
    )
    try:
        firmware_notification_request = requests.post(
            "http://iot-thing-configurator.iot.svc.cluster.local:8088/update_firmware",
            json=k8s_message,
        )
        firmware_notification_request.raise_for_status()
    except requests.exceptions.RequestException as err:
        logger.error(
            "Unable to notify the Firmware Fetcher Service due to following error: "
        )
        logger.exception(err)
    return primary_resource_kind


def loop_through_messages_1(attribute_value, k8s_kind, k8s_name):
    if isinstance(attribute_value["StringValue"], dict):
        k8s_message = attribute_value["StringValue"]
    else:
        k8s_message = json.loads(attribute_value["StringValue"])
    k8s_kind, k8s_name, has_multiple_components = check_for_multiple_components(
        k8s_kind, k8s_message, k8s_name
    )
    return has_multiple_components, k8s_kind, k8s_message, k8s_name


def check_for_multiple_components(k8s_kind, k8s_message, k8s_name):
    has_multiple_components = False
    if k8s_kind.find("|") != -1:
        k8s_kind = k8s_kind.split("|")[0]
        has_multiple_components = True
        logger.info("parsed k8sKind {}.".format(k8s_kind))

        try:
            if "metadata" in k8s_message and "name" in k8s_message["metadata"]:
                k8s_name = k8s_message["metadata"]["name"]
                logger.info(f"Name retrieved from the deployment manifest is {k8s_name}")
            else:
                logger.info("Message has no metadata")
        except KeyError as e:
            logger.info("Message does not have metadata name")
            logger.exception(
                f"exception details '{e}'"
            )
    return k8s_kind, k8s_name, has_multiple_components


def process_primary_resource_kind(
        primary_resource_kind,
        has_multiple_components,
        component_name,
        k8s_name,
        k8s_namespace,
        k8s_image_version,
        assets_url,
        asset_restaurant_id,
        previous_deployment,
        k8s_pv_name,
        k8s_pvc_name,
        messages,
):
    if primary_resource_kind == "deployment":

        if has_multiple_components == "False":
            logger.info(
                f"Deployment has single component, setting k8sName to componentName"
            )
            k8s_name = component_name
        else:
            logger.info(
                f"Deployment has multiple components, proceeding to deploying each component individually"
            )

        if wait_for_deployment_to_complete(k8s_name, k8s_namespace, primary_resource_kind):
            update_asset_service(
                component_name,
                k8s_name,
                k8s_image_version,
                assets_url,
                asset_restaurant_id,
                has_multiple_components,
            )
            # update deploymenthistory for component with Completed status
            update_deployment_history(asset_restaurant_id, assets_url, messages, "Completed")
            trigger_open_test(component_name)
        else:
            logger.error(
                f"Deployment for '{k8s_name}' with version '{k8s_image_version}' was unsuccessful!"
            )
            # update deployment history with failed status
            update_deployment_history(asset_restaurant_id, assets_url, messages, "Failed")
            check_for_previous_deployment(
                k8s_name,
                primary_resource_kind,
                k8s_namespace,
                previous_deployment,
                k8s_pv_name,
                k8s_pvc_name,
                asset_restaurant_id,
            )
    elif primary_resource_kind in ["job", "cronjob"]:
        update_asset_service(
            component_name,
            k8s_name,
            k8s_image_version,
            assets_url,
            asset_restaurant_id,
            has_multiple_components,
        )
        # update deploymenthistory for component with Completed status
        update_deployment_history(asset_restaurant_id, assets_url, messages, "Completed")
    elif primary_resource_kind == "certificatedelivery":
        confirm_certificate_delivery(component_name, assets_url, asset_restaurant_id)


def main():
    configure()
    try:
        get_sqs_message()
    except NameError:
        print("Exception error")


if __name__ == "__main__":
    main()
