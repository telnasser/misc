# Detects text in a document stored in an S3 bucket.
import boto3
from botocore.exceptions import ClientError
from time import sleep

BUCKET_NAME = 'media-clone'
ACCESS_KEY = 'AKIARBDHWMK77LZ5SBOQ'
SECRET_KEY = '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'
TXT_CLIENT = boto3.client('textract',
                          region_name='eu-west-1',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)
S3_CLIENT = boto3.resource('s3',
                           aws_access_key_id=ACCESS_KEY,
                           aws_secret_access_key=SECRET_KEY)

S3_BUCKET = S3_CLIENT.Bucket(BUCKET_NAME)

S3_OUTPUT_PREFIX = 'comprehend_files/'


def extract_data_from_file(file_key):
    extracted_text = []
    response = TXT_CLIENT.detect_document_text(
        Document={'S3Object': {'Bucket': BUCKET_NAME, 'Name': file_key}})
    blocks = response['Blocks']
    for block in blocks:
        if block['BlockType'] != 'PAGE':
            print('Detected: ' + block['Text'])
            print('Confidence: ' +
                  "{:.2f}".format(block['Confidence']) + "%")

            block_text = block['Text']
            extracted_text.append(block_text)
    return ' '.join(extracted_text)


def write_text_file_to_s3(file_name, text):
    print("Writing data to file " + text + "=============\n")
    S3_CLIENT.Object(BUCKET_NAME, S3_OUTPUT_PREFIX +
                     file_name + '.txt').put(Body=text)


def write_text_file_to_file(filename, text):
    file_handler = open(filename, '+w')
    file_handler.write(text)

    file_handler.close()


if __name__ == "__main__":
    for s3_file in S3_BUCKET.objects.all():
        print(f'File: {s3_file}')
        #if s3_file.key.startswith(S3_OUTPUT_PREFIX) or \
            #s3_file.key.split('.')[1].lower() not in ('jpg', 'jpeg', 'png', 'gif', 'tif'):
            #print('IGNORED\n======================\n')
            #continue
        #print()
        try:
            # use textract to process s3 file
            extracted_data = extract_data_from_file(s3_file.key)
            # Output txt file to S3 Bucket.
            output_file_name = s3_file.key.split('/')[-1].split('.')[0]
            #write_text_file_to_s3(output_file_name, extracted_data)
            write_text_file_to_file(output_file_name, extracted_data)
        except ClientError as exception:
            print('File is not supported by Textract')
        finally:
            # sleep 500 milli-seconds to prevent ProvisionedThroughputExceededException
            sleep(0.5)
            print('\n=====================================\n')
            

