import json
import boto3
import os
import time
import concurrent.futures
from datetime import datetime
from botocore.exceptions import ClientError


# 调用bedrock claude，添加重试逻辑
def generate_message(bedrock_runtime, model_id, system_prompt, messages, max_tokens, temperature=0.1, top_p=0.1, max_retries=5, initial_backoff=1):
    body = json.dumps(
        {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "temperature": temperature, 
            "top_p": top_p,
            "system": system_prompt,
            "messages": messages
        }  
    )
    
    retry_count = 0
    backoff_time = initial_backoff
    
    while retry_count <= max_retries:
        try:
            response = bedrock_runtime.invoke_model(body=body, modelId=model_id)
            response_body = json.loads(response.get('body').read())
            return response_body
            
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            error_message = e.response.get('Error', {}).get('Message', '')
            
            # 检查是否是可重试的错误
            if error_code in ['ThrottlingException', 'TooManyRequestsException', 'ServiceUnavailableException', 
                             'LimitExceededException', 'ProvisionedThroughputExceededException'] or 'rate' in error_message.lower() or 'limit' in error_message.lower() or 'throttl' in error_message.lower():
                
                if retry_count < max_retries:
                    print(f"Bedrock API rate limit hit. Retrying in {backoff_time} seconds... (Attempt {retry_count + 1}/{max_retries})")
                    time.sleep(backoff_time)
                    # 指数退避策略
                    backoff_time *= 2
                    retry_count += 1
                    continue
            
            # 如果不是可重试的错误或已达到最大重试次数，则抛出异常
            print(f"Bedrock API error: {error_code} - {error_message}")
            raise
            
        except Exception as e:
            # 处理其他可能的异常
            if retry_count < max_retries:
                print(f"Unexpected error when calling Bedrock API: {str(e)}. Retrying in {backoff_time} seconds... (Attempt {retry_count + 1}/{max_retries})")
                time.sleep(backoff_time)
                backoff_time *= 2
                retry_count += 1
                continue
            
            print(f"Failed after {max_retries} retries. Last error: {str(e)}")
            raise
    
    # 如果所有重试都失败
    raise Exception(f"Failed to call Bedrock API after {max_retries} retries")


def get_vtt_translate(bedrock_client, system_prompt, input_data, language, model_id, max_tokens):
    prompt = f'''
    <input_data>tag中内容为由第三方服务基于视频提取出的vtt格式的字幕，严格遵循以下<requirement>中内要将vtt内容翻译成{language}
    <requirement>
    对每一组时间轴内的信息逐个进行翻译，不要做任何其他的信息的扩展;
    严禁跨时间轴做内容的合并/整合，单个时间轴内语句如果不完整,结合上下文,可适当调整,但仍然保持语句拆分;
    严格准守vtt格式要求,按照内容和时间轴顺序输出,保持有时间轴顺序与数量的完全一致;
    不要急,一组组处理，处理内容反复和原信息进行核对后再输出;
    一定不要删减信息;
    不要在输出中添加额外的换行符;
    </requirement>
    <input_data>
    {input_data}
    </input_data>
    '''
    user_message = {"role": "user", "content": prompt}
    assistant_message = {"role": "assistant", "content": "WEBVTT"}
    messages = [user_message, assistant_message]
    
    response = generate_message(bedrock_client, model_id, system_prompt, messages, max_tokens)
    return response


# 处理单个批次的字幕翻译，包含重试逻辑
def process_translation_batch(batch_data, batch_num, system_prompt, language, model_id, bedrock_client, max_tokens):
    # 确保批次数据格式正确
    text_result = '\n\n'.join([segment.strip() for segment in batch_data])
    try:
        response = get_vtt_translate(bedrock_client, system_prompt, text_result, language, model_id, max_tokens)
        result_text = response['content'][0]['text']
        # 清理结果中可能的格式问题
        return result_text.strip()
    except Exception as e:
        print(f"Error processing translation batch {batch_num}: {str(e)}")
        # 如果处理失败，返回原始文本，确保流程不中断
        return text_result


def lambda_handler(event, context):
    try:
        # 配置参数
        MAX_CONCURRENT_THREADS = int(os.environ.get('MAX_CONCURRENT_THREADS', '3'))  # 默认值为3，可通过环境变量配置
        BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '50'))  # 每批处理的字幕数量，默认50条
        MODEL_ID = os.environ.get('MODEL_ID', 'anthropic.claude-3-5-haiku-20241022-v1:0')
        REGION = os.environ.get('AWS_REGION', 'us-west-2')
        MAX_TOKENS = int(os.environ.get('MAX_TOKENS', '4096'))
        
        bucket = event.get('bucket')
        key = event.get('key')
        language_info = event.get('language', 'zh-中文')
        
        # 解析语言代码和名称
        parts = language_info.split('-')
        LANGUAGE_CODE = parts[0]
        TARGET_LANGUAGE = parts[1]
        
        print(f"Processing file for translation: s3://{bucket}/{key}")
        print(f"Configuration: MAX_CONCURRENT_THREADS={MAX_CONCURRENT_THREADS}, BATCH_SIZE={BATCH_SIZE}, TARGET_LANGUAGE={TARGET_LANGUAGE}")
        
        # 创建S3客户端
        s3_client = boto3.client('s3')
        
        # 从S3下载文件
        response = s3_client.get_object(Bucket=bucket, Key=key)
        vtt_content = response['Body'].read().decode('utf-8')
        
        # 处理文件
        text_list = vtt_content.split('\n\n')
        if text_list[0].strip() == 'WEBVTT':
            del(text_list[0])
        
        # 建立bedrock连接
        bedrock_client = boto3.client(service_name='bedrock-runtime', region_name=REGION)
        
        # 翻译系统提示
        system_prompt = '你是一个资深vtt字幕修订师，对视频基于语音提取的字幕进行翻译'
        
        # 准备批次数据
        batches = []
        current_batch = []
        batch_count = 0
        
        for i in range(len(text_list)):
            current_batch.append(text_list[i])
            batch_count += 1
            
            if batch_count == BATCH_SIZE or i == len(text_list) - 1:
                batches.append(current_batch)
                current_batch = []
                batch_count = 0
        
        print(f"Processing {len(batches)} translation batches with ThreadPoolExecutor, max_workers={MAX_CONCURRENT_THREADS}")
        
        # 使用线程池并行处理批次，限制并发数以避免API限制
        max_workers = min(MAX_CONCURRENT_THREADS, len(batches))
        translation_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(
                    process_translation_batch, 
                    batch, 
                    idx, 
                    system_prompt, 
                    TARGET_LANGUAGE,
                    MODEL_ID, 
                    bedrock_client, 
                    MAX_TOKENS
                ): idx for idx, batch in enumerate(batches)
            }
            
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    result = future.result()
                    translation_results.append((batch_idx, result))
                    print(f"Completed translation batch {batch_idx+1}/{len(batches)}")
                except Exception as exc:
                    print(f'Translation batch {batch_idx} generated an exception: {exc}')
                    # 如果处理失败，使用原始文本
                    original_text = '\n\n'.join(batches[batch_idx])
                    translation_results.append((batch_idx, original_text))
        
        # 按批次索引排序结果
        translation_results.sort(key=lambda x: x[0])
        
        # 合并翻译结果
        final_translation = 'WEBVTT\n\n'
        for idx, (_, result) in enumerate(translation_results):
            # 如果结果以WEBVTT开头，去掉这部分
            if result.startswith('WEBVTT'):
                result = result[len('WEBVTT'):].lstrip()
            
            # 处理结果中的多余换行
            result = result.strip()
            
            # 添加到最终结果，只在非最后一个批次后添加双换行
            final_translation += result
            if idx < len(translation_results) - 1:
                final_translation += '\n\n'
        
        # 上传翻译后的文件到S3
        # 获取源文件的目录路径
        dir_path = os.path.dirname(key)
        # 获取上一级目录路径
        parent_dir = os.path.dirname(dir_path) if dir_path else ""
        
        # 获取文件名
        filename = os.path.basename(key)
        base_name, ext = os.path.splitext(filename)
        print(parent_dir)
        # 构建输出路径：上一级目录/subtitle/文件名_语言.vtt
        output_key = f"{parent_dir}subtitle/{base_name}_{LANGUAGE_CODE}{ext}"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=final_translation.encode('utf-8'),
            ContentType='text/vtt'
        )
        
        print(f"Successfully translated and uploaded to s3://{bucket}/{output_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Subtitle translation completed successfully',
                'input_file': key,
                'output_file': output_key,
                'target_language': TARGET_LANGUAGE,
                'batches_processed': len(batches),
                'concurrent_threads': max_workers
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error translating subtitle: {str(e)}'
            })
        }


# 本地测试用
if __name__ == "__main__":
    # 设置环境变量用于本地测试
    os.environ['MAX_CONCURRENT_THREADS'] = '2'  # 本地测试使用较小的并发数
    
    # 测试直接S3触发
    test_event_s3 = {
        'Records': [{
            's3': {
                'bucket': {
                    'name': 'wongxiao-video'
                },
                'object': {
                    'key': 'optimized/source_subtitle.vtt'
                }
            }
        }]
    }
    
    # 测试异步调用
    test_event_async = {
        'language': 'en-英语',
        'bucket': 'wongxiao-video',
        'key': 'optimized/source_subtitle.vtt'
    }
    
    # 调用Lambda处理函数
    # result = lambda_handler(test_event_s3, None)  # 测试S3触发
    result = lambda_handler(test_event_async, None)  # 测试异步调用
    print(result)
