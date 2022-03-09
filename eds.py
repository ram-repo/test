import os
import boto3
import json
import subprocess
import requests
import time
import sys
import logging2
import secure_filename
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

config = {}


def getSQSMessage():
    assetRestaurantIdResponse = requests.get(
        f"https://{config['assetsUrl']}/restaurant_assets/restaurants?filter[name]={config['restaurantId']}",
        auth=config["awsAuth"],
    )
    assetRestaurantIdResponseJson = secure_filename(json.loads(assetRestaurantIdResponse.text))
    assetRestaurantId = assetRestaurantIdResponseJson["data"][0]["id"]

    queueUrl = f"{config['queueBaseUrl']}{config['queuePrefix']}{assetRestaurantId}{config['queueSuffix']}"
    logger.info(f"queueUrl: {queueUrl}")
    logger.info("Entering to process response")
    response = sqs.receive_message(
        QueueUrl=queueUrl,
        AttributeNames=["All"],
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
        VisibilityTimeout=600,  # VisibilityTimeout should be higher than the deployment timeout (300s)
        WaitTimeSeconds=5,
    )
    logger.info("After completition of response process")
    messageId, receiptHandle, messages = getMessageDataFromResponse(response)
    k8sNamespace, componentName, k8sName = getK8sDataFromMessages(messages)
    logger.info(f"Received message with id of {messageId}")

    logger.info(f"Processed message {messages}")

    primaryResourceKind = loopThroughMessages(
        messages,
        assetRestaurantId,
        config["assetsUrl"],
        componentName,
        k8sName,
        k8sNamespace,
    )
    deleteSQSMessage(queueUrl, receiptHandle, messageId)

    if primaryResourceKind in ["statefulset", "daemonset"]:
        triggerOpenTest(k8sName)


def getQueuePrefixAndAssetsUrl(awsAccount):
    # Adding a condition to check the existing Env
    if awsAccount == "283388140277":
        queuePrefix = os.getenv("queuePrefix", "US-EAST-DEV-BRE-ARCH-BRE-")
        assetsUrl = os.environ.get("assetsUrl", "asset.api.dev.bre.mcd.com")
        iotGetCertificateUrl = os.environ.get("iotGetCertificateUrl",
                                              "z13641x6mf.execute-api.us-east-1.amazonaws.com")
        iotRoleARN = os.environ.get("iotRoleARN",
                                    "arn:aws:iam::385911300465:role/US-EAST-DEV-US-BRE-ARCH-IoT_API_Role")

    elif awsAccount == "524430043955":
        queuePrefix = os.getenv("queuePrefix", "US-EAST-PROD-BRE-ARCH-BRE-")
        assetsUrl = os.environ.get("assetsUrl", "asset.api.prod.bre.mcd.com")
        iotGetCertificateUrl = os.environ.get("iotGetCertificateUrl",
                                              "bscp23mys5.execute-api.us-east-1.amazonaws.com")
        iotRoleARN = os.environ.get("iotRoleARN",
                                    "arn:aws:iam::687478879033:role/US-EAST-DEV-US-BRE-ARCH-IoT_API_Role")

    elif awsAccount == "593265675765":
        queuePrefix = os.getenv("queuePrefix", "US-EAST-INT-BRE-ARCH-BRE-")
        assetsUrl = os.environ.get("assetsUrl", "asset.api.int.bre.mcd.com")
    else:
        logger.error("{} AwsAccount is currently not supported ".format(awsAccount))
        sys.exit(1)
    return queuePrefix, assetsUrl, iotGetCertificateUrl, iotRoleARN


def getMessageDataFromResponse(response):
    messages = {}
    messagePayload = {}
    logger.info("Message Response: " + str(response))
    try:
        message = response["Messages"][0]
        messageId = message["MessageId"]
        receiptHandle = message["ReceiptHandle"]
        # try to get the payload in the message body, if not, use message attributes.
        if isValidJson(message["Body"]):
            messagePayload = message["Body"]
            messages = secure_filename(json.load(StringIO(messagePayload)))
        else:
            messagePayload = message["MessageAttributes"]
            messages = secure_filename(json.loads(json.dumps(messagePayload)))
    except KeyError:
        logger.info("No messages in queue")
        exit(0)
    return messageId, receiptHandle, messages


def getK8sDataFromMessages(messages):
    # print(type(messages["metadata"]["StringValue"]["metadata"]))
    try:
        if isinstance(messages["metadata"]["StringValue"], dict):
            messageMetadata = messages["metadata"]["StringValue"]["metadata"]
        else:
            messageMetadata = secure_filename(json.loads(messages["metadata"]["StringValue"]))["metadata"]
        k8sNamespace = messageMetadata["namespace"]
        logger.info(
            f"Namespace retrieved from the deployment manifest is {k8sNamespace}"
        )
        componentName = messageMetadata["pod"]
        logger.info(
            f"Component name retrieved from the deployment manifest is {componentName}"
        )
        k8sName = componentName
    except KeyError:
        k8sNamespace = ""
        k8sName = ""
        componentName = ""
        logger.info("No metadata attribute")
    return k8sNamespace, componentName, k8sName


def getNamespace(name):
    namespace = name.split("-")
    return namespace[0]


def tryPostingCertificateNotification(messages):
    if isinstance(messages["certificate"]["StringValue"], dict):
        certificateNotification = messages["certificate"]["StringValue"]
    else:
        certificateNotification = secure_filename(json.loads(messages["certificate"]["StringValue"]))
    # From the above payload, we only need ["attributes"]["deviceID"].
    logger.info(
        "Certificate Notification has been triggered. Message contents are {}".format(
            certificateNotification
        )
    )
    logger.info("group Id{}".format(certificateNotification['groupId']))
    onboardJson = {
        'deviceId': certificateNotification['deviceId'],
        'restaurantId': certificateNotification['restaurantId'],
        'pod': certificateNotification['componentName'],
        'namespace': getNamespace(certificateNotification['componentName']),
        'componentId': certificateNotification['componentId'],
        'rotation': certificateNotification['rotation'],
        'groupId': certificateNotification['groupId']
    }
    
    if certificateNotification['rotation']:
        onboardJson['componentConfigId'] = certificateNotification['componentConfigId']

    iotUrl = "https://" + config["iotGetCertificateUrl"] + "/dev/on-boarding/certificate"
    logger.info("iot URL{}".format(iotUrl))
    # Send certificate notification POST request to IoT Edge Certificate Generator.
    try:
        certificateNotificationRequest = requests.post(
            iotUrl,
            json=onboardJson,
            auth=config["awsAuthIoT"]
        )
        logger.info("response {}".format(certificateNotificationRequest.text))
    except requests.exceptions.RequestException as err:
        logger.error("Unable to notify the IoT Get Certificate Handler.")
        logger.exception(err)


def checkForPreviousDeployment(
        k8sName,
        primaryResourceKind,
        k8sNamespace,
        previousDeployment,
        k8sPvName,
        k8sPvcName,
        assetRestaurantId,
):
    if previousDeployment:
        versionToRollBackTo = getImageVersionFromDeployment(previousDeployment)
        logger.info(
            f"Rolling back deployment '{k8sName}' to version '{versionToRollBackTo}'..."
        )
        applyk8sMessage(
            k8sName,
            primaryResourceKind,
            k8sNamespace,
            previousDeployment,
            k8sPvName,
            k8sPvcName,
            assetRestaurantId,
        )
        logger.info(
            f"Successfully rolled back deployment '{k8sName}' to version '{versionToRollBackTo}'"
        )
    else:
        logger.info(
            f"Cannot roll-back deployment '{k8sName}', as this is a first-time deployment."
        )


def isValidJson(payload):
    try:
        jsonObject = secure_filename(json.loads(payload))
    except ValueError as e:
        return False
    return True


def getImageVersionFromDeployment(deployment):
    if not isinstance(deployment, client.V1Deployment):
        # if deployment isn't already a V1Deployment object, then assume it's a dictionary and parse
        return deployment["spec"]["template"]["spec"]["containers"][0]["image"].split(
            ":"
        )[1]
    else:
        return deployment.spec.template.spec.containers[0].image.split(":")[1]


def getImageVersionFromJob(job):
    return job["spec"]["template"]["spec"]["containers"][0]["image"].split(":")[1]


def getImageVersionFromCronJob(cronjob):
    return cronjob["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0][
        "image"
    ].split(":")[1]


def applyk8sMessage(
        k8sName, k8sKind, k8sNamespace, k8sMessage, k8sPvName, k8sPvcName, assetRestaurantId
):
    try:
        coreV1Api.create_namespace(
            client.V1Namespace(metadata=client.V1ObjectMeta(name=k8sNamespace))
        )

        clusterNameInfo = coreV1Api.read_namespaced_config_map(
            CLUSTER_NAME_CONFIG_MAP, BRED_NAMESPACE
        )
        configMapJson = secure_filename(json.loads(
            json.dumps(
                {
                    "apiVersion": "v1",
                    "data": clusterNameInfo.data,
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": CLUSTER_NAME_CONFIG_MAP,
                        "namespace": k8sNamespace,
                    },
                },
                sort_keys=False,
                indent=None,
            )
        ))

        coreV1Api.create_namespaced_config_map(
            k8sNamespace, configMapJson, pretty="pretty"
        )

    except client.rest.ApiException as e:
        if e.status == 409:
            logger.debug("ns/{} already exists... skipping!".format(k8sNamespace))
    else:
        logger.info("created ns/{}".format(k8sNamespace))
        # copying imagepullsecrets to new NS took up to 13 seconds to apply
        sleep(20)
    try:
        createResource(k8sKind, k8sNamespace, k8sMessage)
    except client.rest.ApiException as e:
        if e.status == 409:
            replaceResource(
                k8sName, k8sKind, k8sNamespace, k8sMessage, k8sPvName, k8sPvcName
            )
        else:
            logger.error(
                f"applying {k8sKind}/{k8sName} in ns/{k8sNamespace} failed! Error code: {e.status}, Reason: {e.reason}."
            )
            logger.error(f"Headers: {e.headers}.")
            logger.error(f"Body: {e.body}.")
    else:
        logger.info("created {}/{} in ns/{}".format(k8sKind, k8sName, k8sNamespace))


def createResource(k8sKind, k8sNamespace, k8sMessage):
    # initial create resource
    if k8sKind == "deployment":
        appsV1Api.create_namespaced_deployment(
            k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "service":
        coreV1Api.create_namespaced_service(k8sNamespace, k8sMessage, pretty="pretty")
    elif k8sKind == "ingress":
        extensionsV1beta1Api.create_namespaced_ingress(
            k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "cronjob":
        batchV1beta1Api.create_namespaced_cron_job(
            k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "job":
        batchV1Api.create_namespaced_job(k8sNamespace, k8sMessage, pretty="pretty")
    elif k8sKind == "persistentvolume":
        coreV1Api.create_persistent_volume(k8sMessage, pretty="pretty")
    elif k8sKind == "persistentvolumeclaim":
        coreV1Api.create_namespaced_persistent_volume_claim(
            k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "role":
        rbacAuthorizationV1Api.create_namespaced_role(
            k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "rolebinding":
        rbacAuthorizationV1Api.create_namespaced_role_binding(
            k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "serviceaccount":
        coreV1Api.create_namespaced_service_account(
            k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind in ("secret", "certificatedelivery"):
        coreV1Api.create_namespaced_secret(k8sNamespace, k8sMessage, pretty="pretty")
    elif k8sKind == "configmap":
        coreV1Api.create_namespaced_config_map(
            k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "ingressrouteudp":
        apiGroupVersion = k8sMessage["apiVersion"]
        apiGroup = apiGroupVersion.split("/")[0]
        apiVersion = apiGroupVersion.split("/")[1]
        plurals = f"{k8sKind.lower()}s"
        logger.info(
            f"creating ingressrouteudp with apiGroup={apiGroup}, apiVersion={apiVersion}, k8sNamespace={k8sNamespace}, plurals={plurals}"
        )
        customV1Api.create_namespaced_custom_object(
            apiGroup, apiVersion, k8sNamespace, plurals, k8sMessage, pretty="pretty"
        )
    else:
        logger.error(f"Unsupported resource type {k8sKind}")


def replaceResource(k8sName, k8sKind, k8sNamespace, k8sMessage, k8sPvName, k8sPvcName):
    # If resource already exists
    logger.info(
        "{}/{} in ns/{} already exists... replacing".format(
            k8sKind, k8sName, k8sNamespace
        )
    )
    if k8sKind == "deployment":
        appsV1Api.replace_namespaced_deployment(
            k8sName, k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "service":
        conditionallyReplaceNamespacedService(k8sName, k8sNamespace, k8sMessage)
    elif k8sKind == "ingress":
        extensionsV1beta1Api.replace_namespaced_ingress(
            k8sName, k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "cronjob":
        batchV1beta1Api.replace_namespaced_cron_job(
            k8sName, k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "job":
        logger.info(
            "deleting existing {}/{} in ns/{}...".format(k8sKind, k8sName, k8sNamespace)
        )
        batchV1Api.delete_namespaced_job(k8sName, k8sNamespace, pretty="pretty")
        sleep(10)
        logger.info(
            "recreating {}/{} in ns/{}...".format(k8sKind, k8sName, k8sNamespace)
        )
        batchV1Api.create_namespaced_job(k8sNamespace, k8sMessage, pretty="pretty")
    elif k8sKind == "persistentvolumeclaim":
        coreV1Api.patch_namespaced_persistent_volume_claim(
            k8sPvcName, k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "persistentvolume":
        coreV1Api.patch_persistent_volume(k8sPvName, k8sMessage, pretty="pretty")
    elif k8sKind == "role":
        rbacAuthorizationV1Api.replace_namespaced_role(
            k8sName, k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "rolebinding":
        rbacAuthorizationV1Api.replace_namespaced_role_binding(
            k8sName, k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "serviceaccount":
        coreV1Api.replace_namespaced_service_account(
            k8sName, k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind in ("secret", "certificatedelivery"):
        coreV1Api.replace_namespaced_secret(
            k8sName, k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "configmap":
        coreV1Api.replace_namespaced_config_map(
            k8sName, k8sNamespace, k8sMessage, pretty="pretty"
        )
    elif k8sKind == "ingressrouteudp":
        apiGroupVersion = k8sMessage["apiVersion"]
        apiGroup = apiGroupVersion.split("/")[0]
        apiVersion = apiGroupVersion.split("/")[1]
        plurals = f"{k8sKind.lower()}s"
        logger.info(
            f"replacing ingressrouteudp with apiGroup={apiGroup}, apiVersion={apiVersion}, k8sNamespace={k8sNamespace}, plurals={plurals}"
        )
        try:
            logger.info(
                "patching {}/{} in ns/{}...".format(k8sKind, k8sName, k8sNamespace)
            )
            customV1Api.patch_namespaced_custom_object(
                apiGroup, apiVersion, k8sNamespace, plurals, k8sName, k8sMessage
            )
        except client.rest.ApiException as e:
            logger.info(
                "patch failed. replacing {}/{} in ns/{}...".format(
                    k8sKind, k8sName, k8sNamespace
                )
            )
            customV1Api.replace_namespaced_custom_object(
                apiGroup, apiVersion, k8sNamespace, plurals, k8sName, k8sMessage
            )
    else:
        logger.error(f"Unsupported resource type {k8sKind}")


def conditionallyReplaceNamespacedService(k8sName, k8sNamespace, k8sMessage):
    if "resourceVersion" in k8sMessage and not k8sMessage["resourceVersion"]:
        k8sMessage.pop("resourceVersion")
        coreV1Api.replace_namespaced_service(
            k8sName, k8sNamespace, k8sMessage, pretty="pretty"
        )


def triggerOpenTest(k8sName):
    logger.info("Triggering OpenTest integration test")
    try:
        openTestCronJob = batchV1beta1Api.read_namespaced_cron_job(
            "qe-opentest-integration", "qe"
        )
        openTestJobSpec = openTestCronJob.spec.job_template.spec
        openTestJobSpec.template.spec.containers[1].env.append(
            client.V1EnvVar(name="DEPLOYMENT_NAME", value=k8sName)
        )
        # max length is 63 chars
        jobName = f"opentest-{k8sName}-{uuid4().hex[0:16]}"[0:63]

        newOpenTestJob = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.models.V1ObjectMeta(name=jobName),
            spec=openTestJobSpec,
        )

        batchV1Api.create_namespaced_job("qe", newOpenTestJob, pretty="pretty")
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


def deleteSQSMessage(sqsQueue, receiptHandle, messageId):
    # Delete received message from queue
    logger.info(f"Deleting sqs message with id of {messageId}")
    sqs.delete_message(QueueUrl=sqsQueue, ReceiptHandle=receiptHandle)


def waitForDeploymentToComplete(
        k8sName, k8sNamespace, primaryResourceKind, timeout=300
):
    start = time.time()
    logger.info(f"Waiting for deployment '{k8sName}' to succeed...")
    logger.info(f"Resource kind is : {primaryResourceKind}")
    while time.time() - start < timeout:
        sleep(2)
        try:
            # Wait for deployment only
            if primaryResourceKind != "deployment":
                return False
            deployment = appsV1Api.read_namespaced_deployment_status(
                k8sName, k8sNamespace
            )
        except client.rest.ApiException as e:
            # Handles 404 or 401 or 400 error
            logger.error(
                f"applying {k8sName} in ns/{k8sNamespace} failed! Error code: {e.status}, Reason: {e.reason}."
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
            logger.info(f"Successfully deployed '{k8sName}'!")
            return deployment
        else:
            elapsedSeconds = time.time() - start
            logger.info(
                f"Updated replicas: {status.updated_replicas} of {deployment.spec.replicas}... ({int(elapsedSeconds)}/{timeout}s)"
            )

    logger.error(f"Deployment did not complete after {timeout} seconds")
    return False


def getComponentId(assetRestaurantId, assetsUrl, k8sName):
    assetsUrlParams = {
        "filter[name]": k8sName,
        "filter[restaurant.id]": assetRestaurantId,
        "fields[components]": "id",
    }
    assetsUrlResponse = requests.get(
        f"{https_prefix}{assetsUrl}/restaurant_assets/components",
        params=assetsUrlParams,
        auth=config["awsAuth"],
    )
    assetsUrlResponseJson = secure_filename(json.loads(assetsUrlResponse.text))
    componentId = assetsUrlResponseJson["data"][0]["id"]

    return componentId


# get deployment history id for the component
def getDeploymentDetails(assetRestaurantId, assetsUrl, groupId):
    historyParams = {
        "filter[restaurant.id]": assetRestaurantId,
        "filter[deployment_group.id]": groupId,
    }
    historyResponse = requests.get(
        f"{https_prefix}{assetsUrl}/restaurant_assets/deployment_history",
        params=historyParams,
        auth=config["awsAuth"],
    )
    historyResponseJson = secure_filename(json.loads(historyResponse.text))
    historyID = historyResponseJson["data"][0]["id"]
    return historyID


# get deployment group id from the message metadata
def getDeploymentGroupID(messages):
    msgMetadata = secure_filename(json.loads(messages["metadata"]["StringValue"]))["metadata"]

    if "deploymentGroupId" in msgMetadata:
        deploymentID = msgMetadata["deploymentGroupId"]
    else:
        deploymentID = "0"
    logger.info(
        f"Deployment group id name from the deployment manifest is {deploymentID}"
    )
    return deploymentID


def updateAssetService(
        componentName,
        k8sName,
        deploymentVersion,
        assetsUrl,
        assetRestaurantId,
        hasMultipleComponents,
):
    if "restaurant-assets" in k8sName:
        # after we deploy asset svc, we need to let it restart before running queries
        sleep(60)

    if hasMultipleComponents:
        componentId = getComponentId(assetRestaurantId, assetsUrl, componentName)
    else:
        componentId = getComponentId(assetRestaurantId, assetsUrl, k8sName)

    response = requests.get(
        f"{https_prefix}{assetsUrl}/restaurant_assets/components/{componentId}",
        auth=config["awsAuth"],
    )

    payload = {
        "data": {
            "type": "components",
            "attributes": {"reportedVersion": deploymentVersion},
        }
    }
    headers = {"content-type": "application/vnd.api+json"}

    if response.status_code != 200:
        logger.error("API Endpoint is currently not responding")
    else:
        logger.info(
            "Attempting to patch component name "
            + k8sName
            + " with version: "
            + deploymentVersion
        )
        requests.patch(
            f"{https_prefix}{assetsUrl}/restaurant_assets/components/{componentId}",
            data=json.dumps(payload),
            headers=headers,
            auth=config["awsAuth"],
        )

        logger.info("Patching Assets API successful!")


# code to update deployment history
def updateDepoymentHistory(assetRestaurantId, assetsUrl, messages, status):
    groupid = getDeploymentGroupID(messages)

    if groupid == "0":
        logger.info("No need to update deployment history")
    else:
        # get deployment history details
        historyID = getDeploymentDetails(assetRestaurantId, assetsUrl, groupid)
        logger.info("Deployment history to be updated: " + historyID)

        payload = {
            "data": {"type": "deployment_history", "attributes": {"status": status}}
        }
        headers = {"content-type": "application/vnd.api+json"}

        apiResponse = requests.patch(
            f"{https_prefix}{assetsUrl}/restaurant_assets/deployment_history/{historyID}",
            data=json.dumps(payload),
            headers=headers,
            auth=config["awsAuth"],
        )
        logger.info("Patching Deployment Status successful!")

        if apiResponse.status_code == 200:
            logger.error(
                "Deployment status updated for deployment history id "
                + historyID
                + " with status "
                + status
            )
        else:
            logger.info(
                "Deployment status updation failed for history id: " + historyID
            )


def confirmCertificateDelivery(k8sName, assetsUrl, assetRestaurantId):
    componentId = getComponentId(assetRestaurantId, assetsUrl, k8sName)

    assetsUrlParams = {
        "filter[propertyName]": "OnboardingStatus",
        "filter[component.id]": componentId,
        "fields[component_props]": "id",
    }
    propertyResponse = requests.get(
        f"{https_prefix}{assetsUrl}/restaurant_assets/component_props",
        params=assetsUrlParams,
        auth=config["awsAuth"],
    )
    propertyResponseJson = secure_filename(json.loads(propertyResponse.text))
    propertyId = propertyResponseJson["data"][0]["id"]

    payload = {
        "data": {"type": "component_props", "attributes": {"propertyValue": "Success"}}
    }
    headers = {"content-type": "application/vnd.api+json"}

    if propertyResponse.status_code != 200:
        logger.error("API Endpoint is currently not responding")
    else:
        logger.info(
            "Attempting to patch OnboardingStatus to Completed for component name "
            + k8sName
        )
        requests.patch(
            f"{https_prefix}{assetsUrl}/restaurant_assets/component_props/{propertyId}",
            data=json.dumps(payload),
            headers=headers,
            auth=config["awsAuth"],
        )
        logger.info("Patching Assets API successful!")


def getTemporaryCredentials(accessKey, secretKey, iotRoleARN):
    session = boto3.Session(
        aws_access_key_id=accessKey,
        aws_secret_access_key=secretKey,
    )
    sts_client = session.client('sts')

    assumed_role_object = sts_client.assume_role(
        RoleArn=iotRoleARN,
        RoleSessionName="AssumeRoleSession1"
    )

    credentials = assumed_role_object['Credentials']
    return credentials


def configure():
    if "restaurant_id" in os.environ:
        restaurantId = str(os.getenv("restaurant_id"))
        logger.info(f"Restaurant id: {restaurantId}")
    else:
        logger.error("'restaurant_id' env variable does not exist")
        sys.exit(1)

    # Setting up Env variable for AwsAccount , Region
    awsRegion = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    awsAccount = os.getenv("AWS_ACCOUNT", "283388140277")
    awsAccessKey = os.getenv("AWS_ACCESS_KEY_ID")
    awsSecretKey = os.getenv("AWS_SECRET_ACCESS_KEY")

    queueBaseUrl = f"https://sqs.{awsRegion}.amazonaws.com/{awsAccount}/"
    logger.info(f"Base SQS Queue URL: {queueBaseUrl}")
    queueSuffix = "-SQS-DEPLOYMENT.fifo"

    queuePrefix, assetsUrl, iotGetCertificateUrl, iotRoleARN = getQueuePrefixAndAssetsUrl(awsAccount)

    awsAuth = BotoAWSRequestsAuth(
        aws_host=assetsUrl, aws_region=awsRegion, aws_service="execute-api"
    )
    credentials = getTemporaryCredentials(awsAccessKey, awsSecretKey, iotRoleARN)

    awsAuthIoT = AWSRequestsAuth(credentials["AccessKeyId"], credentials["SecretAccessKey"],
                                 iotGetCertificateUrl, 'us-east-1', 'execute-api',
                                 credentials["SessionToken"])

    assetRestaurantIdResponse = requests.get(
        f"https://{assetsUrl}/restaurant_assets/restaurants?filter[name]={restaurantId}",
        auth=awsAuth,
    )

    assetsUrlResponseJson = secure_filename(json.loads(assetRestaurantIdResponse.text))
    logger.info(f"Restaurant details : {assetsUrlResponseJson}")

    assetRestaurantId = assetsUrlResponseJson["data"][0]["id"]

    config["restaurantId"] = restaurantId
    config["awsAccount"] = awsAccount
    config["awsRegion"] = awsRegion
    config["queueBaseUrl"] = queueBaseUrl
    config["queueSuffix"] = queueSuffix
    config["assetsUrl"] = assetsUrl
    config["queuePrefix"] = queuePrefix
    config["awsAuth"] = awsAuth
    config["assetRestaurantId"] = assetRestaurantId
    config["iotGetCertificateUrl"] = iotGetCertificateUrl
    config["awsAuthIoT"] = awsAuthIoT


def loopThroughMessages(
        messages, assetRestaurantId, assetsUrl, componentName, k8sName, k8sNamespace
):
    # set to None by default so we can recognize cases where a SQS message does not change a primary resource
    primaryResourceKind = None
    k8sPvName = None
    k8sPvcName = None
    k8sImageVersion = None
    previousDeployment = None
    # svcs,svcs-restaurant-assets,svcs-restaurant-assets
    for attributeName, attributeValue in messages.items():
        k8sKind = attributeName.lower()
        if isinstance(attributeValue["StringValue"], dict):
            k8sMessage = attributeValue["StringValue"]
        else:
            k8sMessage = secure_filename(json.loads(attributeValue["StringValue"]))
        k8sKind, k8sName, hasMultipleComponents = checkForMultipleComponents(
            k8sKind, k8sMessage, k8sName
        )
        if k8sKind == "cronjob":
            primaryResourceKind = k8sKind
            k8sImageVersion = getImageVersionFromCronJob(k8sMessage)
        elif k8sKind == "secret":
            k8sName = k8sMessage["metadata"]["name"]
            k8sNamespace = k8sName.split("-")[0]
        elif k8sKind == "configmap":
            k8sName = k8sMessage["metadata"]["name"]
            k8sNamespace = k8sName.split("-")[0]
        elif k8sKind == "certificate":
            primaryResourceKind = k8sKind
            tryPostingCertificateNotification(messages)
        elif k8sKind == "certificatedelivery":
            primaryResourceKind = k8sKind
            k8sName = k8sMessage["metadata"]["name"]
            k8sNamespace = k8sName.split("-")[0]
        elif k8sKind == "firmware":
            primaryResourceKind = k8sKind
            logger.info(
                "Firmware Notification has been triggered. Message contents are {}".format(
                    k8sMessage
                )
            )
            try:
                # firmwareNotificationRequest = requests.post(
                #     "http://iot-firmware-fetcher.iot.svc.cluster.local:8080/updateFirmwareVersion",
                #     json=k8sMessage,
                # )
                firmwareNotificationRequest = requests.post(
                    "http://thing-configurator.iot.svc.cluster.local:8088/update_firmware",
                    json=k8sMessage,
                )
                firmwareNotificationRequest.raise_for_status()
            except requests.exceptions.RequestException as err:
                logger.error(
                    "Unable to notify the Firmware Fetcher Service due to following error: "
                )
                logger.exception(err)
        elif k8sKind == "persistentvolume":
            k8sPv = k8sMessage
            k8sPvName = k8sPv["metadata"]["name"]
        elif k8sKind == "persistentvolumeclaim":
            k8sPvc = k8sMessage
            k8sPvcName = k8sPvc["metadata"]["name"]
        elif k8sKind == "ingress":
            primaryResourceKind = k8sKind
        elif k8sKind == "deployment":
            primaryResourceKind = k8sKind
            k8sImageVersion = getImageVersionFromDeployment(k8sMessage)
            #
            logger.info(f"Found k8s image version : {k8sImageVersion}")

            # save current deployment before applying new one, so we can easily roll-back to it if needed
            try:
                previousDeployment = appsV1Api.read_namespaced_deployment(
                    k8sName, k8sNamespace, export=True
                )
            except:
                previousDeployment = None
                logger.info(
                    f"Did not find existing deployment with name '{k8sName}': has not been deployed before"
                )

        elif k8sKind == "job":
            primaryResourceKind = k8sKind
            k8sImageVersion = getImageVersionFromJob(k8sMessage)

        # BTP-231:
        # this k8s kind "onboarddevice" deals with onboarding/action of iot(eg: greengrass) or non-iot(eg: axis cam) devices
        # takes onboarding payload from onboarding lambda through SQS queue and passes them to iot-thing-configurator
        elif k8sKind == "onboarddevice":
            primaryResourceKind = k8sKind
            itcEndpoint = "http://thing-configurator.iot.svc.cluster.local:8088/configurator_action"  # iot thing configurator endpoint
            # itcEndpoint = "http://iot-thing-configurator.iot.svc.cluster.local:8088/configurator_action"  # iot thing configurator endpoint
            logger.info(
                "Preparing to trigger iot-thing-configurator, Request payload= {}, Endpoint= {}".format(k8sMessage,
                                                                                                        itcEndpoint)
            )
            try:
                itcResponse = requests.post(
                    itcEndpoint,
                    json=k8sMessage,  # K8s message containing onboarding/action payload
                )

                logger.info(
                    "iot-thing-configurator triggered, Response code = {}, Response = {}".format(
                        itcResponse.status_code,
                        itcResponse)
                )

            except requests.exceptions.RequestException as err:
                logger.error(
                    "Unable to trigger iot-thing-configurator: "
                )
                logger.exception(err)

        if k8sKind not in ["metadata", "certificate", "firmware", "onboarddevice"]:
            applyk8sMessage(
                k8sName,
                k8sKind,
                k8sNamespace,
                k8sMessage,
                k8sPvName,
                k8sPvcName,
                assetRestaurantId,
            )

        processPrimaryResourceKind(
            primaryResourceKind,
            hasMultipleComponents,
            componentName,
            k8sName,
            k8sNamespace,
            k8sImageVersion,
            assetsUrl,
            assetRestaurantId,
            previousDeployment,
            k8sPvName,
            k8sPvcName,
            messages,
        )
    return primaryResourceKind


def checkForMultipleComponents(k8sKind, k8sMessage, k8sName):
    hasMultipleComponents = False
    if k8sKind.find("|") != -1:
        k8sKind = k8sKind.split("|")[0]
        hasMultipleComponents = True
        logger.info("parsed k8sKind {}.".format(k8sKind))

        try:
            if "metadata" in k8sMessage and "name" in k8sMessage["metadata"]:
                k8sName = k8sMessage["metadata"]["name"]
                logger.info(f"Name retrieved from the deployment manifest is {k8sName}")
            else:
                logger.info("Message has no metadata")
        except KeyError as e:
            logger.info("Message does not have metadata name")
    return k8sKind, k8sName, hasMultipleComponents


def processPrimaryResourceKind(
        primaryResourceKind,
        hasMultipleComponents,
        componentName,
        k8sName,
        k8sNamespace,
        k8sImageVersion,
        assetsUrl,
        assetRestaurantId,
        previousDeployment,
        k8sPvName,
        k8sPvcName,
        messages,
):
    if primaryResourceKind == "deployment":

        if hasMultipleComponents == "False":
            logger.info(
                f"Deployment has single component, setting k8sName to componentName"
            )
            k8sName = componentName
        else:
            logger.info(
                f"Deployment has multiple components, proceeding to deploying each component individually"
            )

        if waitForDeploymentToComplete(k8sName, k8sNamespace, primaryResourceKind):
            updateAssetService(
                componentName,
                k8sName,
                k8sImageVersion,
                assetsUrl,
                assetRestaurantId,
                hasMultipleComponents,
            )
            # update deploymenthistory for component with Completed status
            updateDepoymentHistory(assetRestaurantId, assetsUrl, messages, "Completed")
            triggerOpenTest(componentName)
        else:
            logger.error(
                f"Deployment for '{k8sName}' with version '{k8sImageVersion}' was unsuccessful!"
            )
            # update deployment history with failed status
            updateDepoymentHistory(assetRestaurantId, assetsUrl, messages, "Failed")
            checkForPreviousDeployment(
                k8sName,
                primaryResourceKind,
                k8sNamespace,
                previousDeployment,
                k8sPvName,
                k8sPvcName,
                assetRestaurantId,
            )
    elif primaryResourceKind in ["job", "cronjob"]:
        updateAssetService(
            componentName,
            k8sName,
            k8sImageVersion,
            assetsUrl,
            assetRestaurantId,
            hasMultipleComponents,
        )
        # update deploymenthistory for component with Completed status
        updateDepoymentHistory(assetRestaurantId, assetsUrl, messages, "Completed")
    elif primaryResourceKind == "certificatedelivery":
        confirmCertificateDelivery(componentName, assetsUrl, assetRestaurantId)


def main():
    configure()
    try:
        getSQSMessage()
    except NameError:
        print("Exception error")


if __name__ == "__main__":
    main()
