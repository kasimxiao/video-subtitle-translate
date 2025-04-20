import json
import boto3
import os
import string
import re
import time
import concurrent.futures
from datetime import datetime
from botocore.exceptions import ClientError


# 通过标点符号识别语句是否结束
def ends_with_symbol(text):
    # 英文标点符号
    english_punctuation = string.punctuation
    # 中文标点符号
    chinese_punctuation = r'。？！，、；：""''（）【】《》〈〉…'
    # 创建包含所有标点符号的正则表达式模式
    symbols = re.escape(english_punctuation + chinese_punctuation)
    pattern = r'[' + symbols + r']+$'
    # 使用正则表达式检查字符串是否以符号结尾
    return bool(re.search(pattern, text))


# 对未结束的语句进行时间轴的合并
# 考虑到可能缺失标点符号，定义最多合并时间轴数量
def get_vtt_merge(text_list, merge_num):
    num = 0
    vtt_merge = ''
    start_time = ''
    vtt_temp = ''
    merge_seq = 1
    for i in range(0, len(text_list)):
        temp_list = text_list[i].split('\n')
        vtt_str = temp_list[-1]

        if len(start_time) == 0:
            start_time = temp_list[1].split(' --> ')[0]
        
        end_time = temp_list[1].split(' --> ')[1]

        #合并语句，同时将单条超短语句进行合并
        if (ends_with_symbol(vtt_str) and len(vtt_temp+vtt_str) >= 10) \
            or i == len(text_list) - 1 \
            or (len(vtt_temp+vtt_str) >= 10 and merge_seq >= merge_num):

            vtt_merge = f'{vtt_merge}\n\n{num}\n{start_time} --> {end_time}\n{vtt_temp}{vtt_str}'
            num = num + 1
            vtt_temp = ''
            start_time = ''
            merge_seq = 1
        else:
            vtt_temp = vtt_temp + vtt_str
            merge_seq = merge_seq + 1
        
    return vtt_merge


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


# 将vtt内容基于claude进行优化，断句、同音字词转换、语句连贯性
def get_vtt_opt(bedrock_client, system_prompt, input_data, rownum, model_id, max_tokens):
    prompt = f'''内容<input_data>为提取的一段培训音频字幕,总共有{rownum}行文本，严格格遵循<requirement>对内容进行优化校对，处理后同样保证有{rownum}行文本,最终以String长文本中文输出。
     <requirement>
        1、按照序号对内容逐条进行处理,不完整的语句禁止做上下文内容的合并;
        2、保持序号与原文数值绝对一致;
        3、每处理一行，确认与原文同序号内容的的一致性;
        4、删除口语中用到的语气发音词;
        5、同音字/词基于语义理解进行修正;
        6、不做任何延展和扩写,无法理解的语句保持原样;
        7、需要反复检核结果与原文行数是否一致;
        8、不要急,一行行处理,处理内容反复和原信息进行核对后再输出,保持内容和换行格式的一致;
    </requirement>
    <input_data>
        {input_data}
    </input_data>
    '''
    user_message = {"role": "user", "content": prompt}
    assistant_message = {"role": "assistant", "content": f"保持原语种，以下为优化后的内容:"}
    messages = [user_message, assistant_message]
    
    response = generate_message(bedrock_client, model_id, system_prompt, messages, max_tokens)
    return response


# 处理单个批次的字幕，包含重试逻辑
def process_subtitle_batch(batch_data, batch_num, system_opt, model_id, bedrock_client, max_tokens):
    text_result = '\n'.join(batch_data)
    try:
        response = get_vtt_opt(bedrock_client, system_opt, text_result, len(batch_data), model_id, max_tokens)
        return response['content'][0]['text']
    except Exception as e:
        print(f"Error processing batch {batch_num}: {str(e)}")
        # 如果处理失败，返回原始文本，确保流程不中断
        return text_result

# 重新组装VTT文件
def replace_text_content(text_list, opt_list):
    result = []
    
    # 添加VTT头部
    result.append("WEBVTT\n")
    
    for text in text_list:
        # 从原文本中提取序号
        text_num = text.split('\n')[0]
        
        # 在opt_list中查找匹配的内容
        matched = False
        for opt in opt_list:
            # 从opt中提取序号
            opt_num = opt.split('.')[0]
            
            # 如果序号匹配
            if text_num == opt_num:
                # 保持原有的时间戳和序号，但替换文本内容
                time_stamp = text.split('\n')[1]
                new_content = opt.split('.', 1)[1].strip()
                new_text = f"{text_num}\n{time_stamp}\n{new_content}\n"
                result.append(new_text)
                matched = True
                break
        
        # 如果没有找到匹配的内容，保持原文不变
        if not matched:
            result.append(f"{text}\n")

    # 将列表转换为字符串
    return "\n".join(result)


def lambda_handler(event, context):
    try:
        # 配置参数
        MAX_CONCURRENT_THREADS = int(os.environ.get('MAX_CONCURRENT_THREADS', '3'))  # 默认值为3，可通过环境变量配置
        BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '10'))  # 每批处理的字幕数量
        MERGE_NUM = int(os.environ.get('MERGE_NUM', '5'))  # 最长合并的时间轴数量
        MODEL_ID = os.environ.get('MODEL_ID', 'anthropic.claude-3-5-sonnet-20240620-v1:0')
        REGION = os.environ.get('AWS_REGION', 'us-west-2')
        MAX_TOKENS = int(os.environ.get('MAX_TOKENS', '4096'))
        
        # 从S3事件中获取桶名和对象键
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing file: s3://{bucket}/{key}")
        print(f"Configuration: MAX_CONCURRENT_THREADS={MAX_CONCURRENT_THREADS}, BATCH_SIZE={BATCH_SIZE}, MERGE_NUM={MERGE_NUM}")
        
        # 创建S3客户端
        s3_client = boto3.client('s3')
        
        # 从S3下载文件
        response = s3_client.get_object(Bucket=bucket, Key=key)
        text = response['Body'].read().decode('utf-8')
        
        # 处理文件
        text_list = text.split('\n\n')
        if text_list[0].strip() == 'WEBVTT':
            del(text_list[0])
        
        # 时间轴语句合并
        merge_result = get_vtt_merge(text_list, MERGE_NUM)
        
        # 建立bedrock连接
        bedrock_client = boto3.client(service_name='bedrock-runtime', region_name=REGION)
        temperature = 0.1
        top_p = 0.1
        
        # 使用claude 对vtt内容进行优化
        system_opt = '你是一位字幕校对专家，基于上下文的理解对文本逐条校对,完善语句使内容更加通顺,上下文衔接更加连贯,保持相同序号数量的输出。'
        
        # 处理合并后的字幕
        text_list = merge_result.split('\n\n')
        if len(text_list) > 0 and text_list[0].strip() == '':
            del(text_list[0])
        
        # 准备批次数据
        batches = []
        current_batch = []
        
        for i in range(len(text_list)):
            i_text = text_list[i].split('\n')
            current_batch.append(f'{i_text[0]}.{i_text[2]}')
            
            if len(current_batch) == BATCH_SIZE or i == len(text_list) - 1:
                batches.append(current_batch)
                current_batch = []
        
        print(f"Processing {len(batches)} batches with ThreadPoolExecutor, max_workers={MAX_CONCURRENT_THREADS}")
        
        # 使用线程池并行处理批次，限制并发数以避免API限制
        max_workers = min(MAX_CONCURRENT_THREADS, len(batches))
        opt_results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(
                    process_subtitle_batch, 
                    batch, 
                    idx, 
                    system_opt, 
                    MODEL_ID, 
                    bedrock_client, 
                    MAX_TOKENS
                ): idx for idx, batch in enumerate(batches)
            }
            
            for future in concurrent.futures.as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    result = future.result()
                    opt_results.append((batch_idx, result))
                    print(f"Completed batch {batch_idx+1}/{len(batches)}")
                except Exception as exc:
                    print(f'Batch {batch_idx} generated an exception: {exc}')
                    # 如果处理失败，使用原始文本
                    original_text = '\n'.join(batches[batch_idx])
                    opt_results.append((batch_idx, original_text))
        
        # 按批次索引排序结果
        opt_results.sort(key=lambda x: x[0])
        opt_result = ''.join([result for _, result in opt_results])
        
        # 清理和格式化结果
        opt_result = "\n".join(filter(lambda x: x.strip(), opt_result.splitlines()))
        opt_list = opt_result.split('\n')
        
        final_vtt = replace_text_content(text_list, opt_list)
        
        # 上传优化后的文件到S3
        # 获取源文件的目录路径
        source_dir = os.path.dirname(key)
        # 获取源文件的父目录路径
        parent_dir = os.path.dirname(source_dir)
        # 构建输出路径：父目录/optimized/文件名
        output_key = f"{parent_dir}/optimized/{os.path.basename(key)}"
        s3_client.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=final_vtt.encode('utf-8'),
            ContentType='text/vtt'
        )
        
        print(f"Successfully processed and uploaded to s3://{bucket}/{output_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Subtitle optimization completed successfully',
                'input_file': key,
                'output_file': output_key,
                'batches_processed': len(batches),
                'concurrent_threads': max_workers
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error processing subtitle: {str(e)}'
            })
        }
