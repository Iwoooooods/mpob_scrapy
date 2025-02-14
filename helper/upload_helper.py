# coding=utf8
import logging
import datetime
import zipfile
import os
from helper.ftp_helper import FtpService


def upload_csv_to_ftp(local_file_path, tag, settings):
    logging.info('%s uploading...' % tag)
    zip_filepath = compress_file(local_file_path)
    basename = '%s_%s' % (datetime.datetime.now().strftime('%Y%m%d'), os.path.basename(zip_filepath))
    remote_name = os.path.join(settings.get('BASE_DIR'), tag, basename)
    remote_name = remote_name.replace('\\', '/')   # for windows only
    ftp_service = FtpService(settings.get('HOST'), settings.get('PORT'), settings.get('USERNAME'), settings.get('PASSWORD'))
    ftp_service.upload(zip_filepath, remote_name)
    os.remove(zip_filepath)
    logging.info('%s uploaded.' % tag)


def compress_file(file_path):
    parent_dir = os.path.dirname(file_path)
    basename = os.path.basename(file_path)
    zip_filepath = os.path.join(parent_dir, basename.replace('.csv', '.zip'))
    with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(file_path, arcname=basename)
    return zip_filepath
