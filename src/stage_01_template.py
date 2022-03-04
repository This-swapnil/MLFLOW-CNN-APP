import argparse
import logging
import os
import random
import shutil
import urllib.request as req
from email import header

from tqdm import tqdm

from src.utils.common import create_directories, read_yaml, unzip_file
from src.utils.data_mgmt import validate_image

STAGE = "get data stage"  # <<< change stage name

logging.basicConfig(
    filename=os.path.join("logs", "running_logs.log"),
    level=logging.INFO,
    format="[%(asctime)s: %(levelname)s: %(module)s]: %(message)s",
    filemode="a",
)


def main(config_path):
    # read config files
    config = read_yaml(config_path)
    URL = config["data"]["source_url"]
    local_dir = config["data"]["local_dir"]
    create_directories([local_dir])

    data_file = config["data"]["data_file"]
    data_file_path = os.path.join(local_dir, data_file)

    if not os.path.isfile(data_file_path):
        logging.info("downloading started.....")
        filename, headers = req.urlretrieve(URL, data_file_path)
        logging.info(f"filename:{filename} created with info\n {headers}")
    else:
        logging.info(f"filename:{data_file} already present")

    # Unzip
    unzip_data_dir = config["data"]["unzip_data_dir"]
    if not os.path.exists(unzip_data_dir):
        create_directories([unzip_data_dir])
        unzip_file(source=data_file_path, dest=unzip_data_dir)
    else:
        logging.info(f"data already extracted")

    #validate data
    validate_image(config)


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", "-c", default="configs/config.yaml")
    parsed_args = args.parse_args()

    try:
        logging.info("\n********************")
        logging.info(f">>>>> stage {STAGE} started <<<<<")
        main(config_path=parsed_args.config)
        logging.info(f">>>>> stage {STAGE} completed!<<<<<\n")
    except Exception as e:
        logging.exception(e)
        raise e
