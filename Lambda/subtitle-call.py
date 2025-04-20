import boto3
import re
import json

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    # 获取 S3 对象键
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # 检查对象键是否匹配所需模式
    source_pattern = r'^[^/]+/source/.*\.vtt$'
    optimized_pattern = r'^[^/]+/optimized/.*\.vtt$'

    if re.match(source_pattern, key):
        # 字幕优化
        lambda_client.invoke(
            FunctionName='arn:aws:lambda:{region}:{userid}:function:subtitle-optimization',
            InvocationType='Event',
            Payload=json.dumps(event)
        )
    elif re.match(optimized_pattern, key):
        # 字幕翻译
        language = ['zh-简体中文','tr-土耳其'] 
        #循环异步调用
        for i in language:
            payload = {
                'language': i,
                'bucket': bucket,
                'key': key
            }
            lambda_client.invoke(
                FunctionName='arn:aws:lambda:{region}:{userid}:function:subtitle-translate',
                InvocationType='Event',
                Payload=json.dumps(payload)
            )   
    # 如果不匹配任何模式，则不执行任何操作

    return {
        'statusCode': 200,
        'body': f'Processed {key}'
    }