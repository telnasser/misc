import boto3
import textract
from textract.exceptions import ExtensionNotSupported, ShellError
from zipfile import BadZipFile
from botocore.exceptions import ClientError
from time import sleep

BUCKET_NAME = 'media-clone'
ACCESS_KEY = ''
SECRET_KEY = 'j'
TXT_CLIENT = boto3.client('textract',
                          region_name='eu-west-1',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)
S3_RESOURCE = boto3.resource('s3',
                             aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY)
S3_BUCKET = S3_RESOURCE.Bucket(BUCKET_NAME)
S3_CLIENT = boto3.client('s3',
                         aws_access_key_id=ACCESS_KEY,
                         aws_secret_access_key=SECRET_KEY)
S3_OUTPUT_PREFIX = 'comprehend_files/'


def extract_data_from_image(file_key, **kwargs):
    extracted_text = []
    if kwargs.get('method', None) == 's3':
        response = TXT_CLIENT.detect_document_text(
            Document={'S3Object': {'Bucket': BUCKET_NAME, 'Name': file_key}})
        blocks = response['Blocks']
        for block in blocks:
            if block['BlockType'] != 'PAGE':
                block_text = block['Text']
                extracted_text.append(block_text)
        extracted_text = ' '.join(extracted_text)
    else:
        raise NotImplementedError()
    return extracted_text


def extract_data_from_doc(file_key, **kwargs):
    text = ''
    if kwargs.get('method', None) == 's3':
        file_extension = file_key.split('.')[1]
        with open('tmp', 'wb+') as tmp_file:
            file_content = S3_RESOURCE.Object(BUCKET_NAME, file_key).get()['Body'].read()
            tmp_file.write(file_content)
        text = textract.process('tmp', extension=file_extension)
    else:
        raise NotImplementedError()

    return text


def extract_text_from_file(file_key, **kwargs):
    file_extension = file_key.split('.')[1]
    text = ''
    if file_extension in ('jpg', 'jpeg', 'png', 'gif', 'tif'):
        text = extract_data_from_image(file_key, **kwargs)
    else:
        text = extract_data_from_doc(file_key, **kwargs)
    return text


def write_text_file_to_s3(file_name, text):
    S3_RESOURCE.Object(BUCKET_NAME, S3_OUTPUT_PREFIX +
                       file_name + '.txt').put(Body=text)


if __name__ == "__main__":
    file_names = []
    for s3_file in S3_BUCKET.objects.filter():
        file_name = s3_file.key.split('/')[-1]
        file_names.append(file_name)
       # sleep(0.5)
    print(len(file_names))

    files = [obj.key for obj in sorted(file_names, key=get_last_modified, 
    reverse=True)][0:9]
    print(files)
