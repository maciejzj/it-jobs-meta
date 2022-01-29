from it_jobs_meta.data_pipeline.data_formats import NoFluffJObsPostingsData
from it_jobs_meta.data_pipeline.data_warehouse import make_db_uri_from_config, DataWarehouseDbConfig

import boto3

def lambda_handler(event, context):
    
    print('Lambda working')

    string = "dfghj"
    encoded_string = string.encode("utf-8")

    bucket_name = "s3bucketitjobsmeta"
    file_name = "hello.txt"
    s3_path = "100001/20180223/" + file_name

    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=s3_path, Body=encoded_string)

    # c = DataWarehouseDbConfig(
    #     'mysql',
    #     'tmp',
    #     'tmp',
    #     ''
    #     'tmp'
    # )

    # return create_engine(conn_str, connect_args=kw)

    return {
        'message': f'Hello AWS {NoFluffJObsPostingsData.__name__}'
    }