from google.cloud import storage
from google.oauth2 import service_account

def upload_file_to_gcs(bucket_name, source_file_path, destination_blob_name, credentials_path):
    """
    将本地文件上传到指定的 GCS 存储桶。

    参数:
    - bucket_name: GCS 存储桶名称，例如 'cdn.emacsvi.com'
    - source_file_path: 本地文件路径，例如 'local/path/to/file.txt'
    - destination_blob_name: 上传到 GCS 后的文件名称，例如 'uploads/file.txt'
    - credentials_path: 服务账户 JSON 文件路径，例如 'emacsvi.json'
    """

    # 创建认证对象
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # 创建存储客户端
    client = storage.Client(credentials=credentials)

    # 获取存储桶
    bucket = client.bucket(bucket_name)

    # 创建 blob 对象
    blob = bucket.blob(destination_blob_name)

    # 上传文件
    blob.upload_from_filename(source_file_path)

    print(f"文件 '{source_file_path}' 已成功上传到存储桶 '{bucket_name}'，路径为 '{destination_blob_name}'。")

# 示例用法
upload_file_to_gcs(
    bucket_name='cdn.emacsvi.com',
    source_file_path='/home/sp/ads/key',
    destination_blob_name='uploads/key',
    credentials_path='/home/sp/ads/emacsvi.json'
)
