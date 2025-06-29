import boto3
import botocore
print(boto3.__version__)
print(botocore.__version__)

try:
    import aiobotocore
    print(aiobotocore.__version__)
except ImportError:
    print("aiobotocore not installed ✅")
