import boto3, json, logging, os, urllib.parse

logger = logging.getLogger()
logger.setLevel(logging.INFO)
glue_client = boto3.client('glue')

# Configured as environment variable
GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']

def lambda_handler(event, context):
    keys = []
    bucket = None

    # Json configured at test
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        if key.endswith('.parquet'):
            keys.append(key)

    if not keys:
        logger.info("No parquet files in event, skipping.")
        return

    logger.info(f"Processing {len(keys)} file(s) from s3://{bucket}")

    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            '--source_bucket': bucket,
            '--source_keys': ','.join(keys),
        }
    )
    logger.info(f"Glue Job started: {response['JobRunId']}")
