from prefect import flow, get_run_logger
from prefect_azure.blob_storage import AzureBlobStorageContainer

BLOB_STORAGE_PATH = (
    "https://{storage_account_name}.blob.core.windows.net/{container_name}/"
)


@flow
def simple_blob_reader(file_url: str, storage_account_name: str = "kevinstest"):
    blob_container: AzureBlobStorageContainer = AzureBlobStorageContainer.load("data")

    blob_storage_root = BLOB_STORAGE_PATH.format(
        storage_account_name=storage_account_name,
        container_name=blob_container.container_name,
    )

    file = file_url.split(blob_storage_root, 1)[1]

    get_run_logger().info(f"Downloading file {file} from {blob_storage_root}")
    blob_container.download_object_to_path(file, "downloaded_example.txt")
