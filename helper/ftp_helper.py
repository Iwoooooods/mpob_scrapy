# coding=utf-8

import os
import time
# from pathlib import Path
from ftplib import FTP, error_perm, error_temp
import logging
logger = logging.getLogger(__name__)


class FtpUtil:
    @staticmethod
    def get_modify_time(session, pathname):
        try:
            resp = session.sendcmd('MDTM %s' % pathname)
            secs = time.mktime(time.strptime(resp.split()[1], '%Y%m%d%H%M%S')) + time.timezone
            return time.strftime('%Y%m%d %H:%M:%S', time.gmtime(secs))
        except Exception as e:
            return None

    @staticmethod
    def exists(session, pathname):
        return FtpUtil.isfile(session, pathname) or FtpUtil.isdir(session, pathname)

    @staticmethod
    def isfile(session, pathname):
        if FtpUtil.isdir(session, pathname):
            return False
        if FtpUtil.get_modify_time(session, pathname):
            return True
        else:
            return False
        pass

    @staticmethod
    def isdir(session, pathname):
        curr_path = session.pwd()
        try:
            if pathname is not None:
                session.cwd(pathname)
                return True
        except error_perm as e:
            return False
        finally:
            session.cwd(curr_path)

    @staticmethod
    def make_dirs(session, remote_dir):
        path_list = []
        # current = Path(remote_dir)
        # parent = current.parent
        current = remote_dir
        parent = os.path.dirname(current)
        path_list.append(current)
        while current != parent:
            path_list.append(parent)
            current = parent
            # parent = current.parent
            parent = os.path.dirname(current)
        for path in reversed(path_list):
            path_str = '%s' % path
            if not FtpUtil.isdir(session, path_str):
                session.mkd('%s' % path_str)
                logger.debug('create dir: %s' % path_str)
        pass

    @staticmethod
    def upload_file(session, local_filename, remote_filename):
        logger.debug('Upload(%s Byte): %s To: %s' % (os.path.getsize(local_filename), local_filename, remote_filename))
        # FtpUtil.make_dirs(session, Path(remote_filename).parent)
        FtpUtil.make_dirs(session, os.path.dirname(remote_filename))
        fh = open(local_filename, 'rb')
        session.storbinary('STOR %s' % remote_filename, fh)
        fh.close()

    @staticmethod
    def download_file(session, remote_filename, local_filename):
        logger.debug('Download(%s Byte): %s To: %s' % (session.size(remote_filename), remote_filename, local_filename))
        fh = open(local_filename, 'wb')
        session.retrbinary('RETR ' + remote_filename, fh.write)
        fh.close()

    @staticmethod
    def upload_dir(session, local_dir, remote_dir):
        FtpUtil.make_dirs(session, remote_dir)
        for f in os.listdir(local_dir):
            local_path = os.path.join(local_dir, f)
            remote_path = os.path.join(remote_dir, f)
            if os.path.isfile(local_path):
                FtpUtil.upload_file(session, local_path, remote_path)
            elif os.path.isdir(local_path):
                FtpUtil.upload_dir(session, local_path, remote_path)

    @staticmethod
    def download_dir(session, remote_dir, local_dir):
        if not os.path.exists(local_dir):
            os.makedirs(local_dir, exist_ok=True)
        for remote_path in session.nlst(remote_dir):
            # local_path = os.path.join(local_dir, Path(remote_path).name)
            local_path = os.path.join(local_dir, os.path.basename(remote_path))
            if FtpUtil.isdir(session, remote_path):
                FtpUtil.download_dir(session, remote_path, local_path)
            elif FtpUtil.isfile(session, remote_path):
                FtpUtil.download_file(session, remote_path, local_path)


class FtpService(object):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        pass

    def connect(self):
        ftp = FTP(self.host)
        ftp.connect(self.host, self.port)
        ftp.set_pasv(False)
        ftp.encoding = 'utf-8'
        ret = ftp.login(self.username, self.password)
        logger.debug('login: %s' % ret)
        return ftp

    def list_dir(self, parent_path):
        try:
            ftp = self.connect()
            return ftp.nlst(parent_path)
        except Exception as e:
            logger.exception('failed to get_file_list: %s' % parent_path)
        finally:
            ftp.close()

    def make_dir(self, path):
        try:
            ftp = self.connect()
            if not FtpUtil.exists(ftp, path):
                ftp.mkd(path)
        except Exception as e:
            logger.exception('failed to make_dir: [%s]' % path)
        finally:
            ftp.close()

    def upload(self, local_path, remote_path):
        try:
            session = self.connect()
            if os.path.isfile(local_path):
                FtpUtil.upload_file(session, local_path, remote_path)
            elif os.path.isdir(local_path):
                FtpUtil.upload_dir(session, local_path, remote_path)
            else:
                logger.error('not find: %s to upload' % local_path)
        except Exception as e:
            logger.exception('failed to upload from [%s] to [%s]' % (local_path, remote_path))
        finally:
            session.close()

    def download(self, remote_path, local_path):
        try:
            session = self.connect()
            if FtpUtil.isdir(session, remote_path):
                FtpUtil.download_dir(session, remote_path, local_path)
            elif FtpUtil.isfile(session, remote_path):
                FtpUtil.download_file(session, remote_path, local_path)
            else:
                logger.error('not find: %s to download' % remote_path)
        except Exception as e:
            logger.exception('failed to download from [%s] to [%s]' % (remote_path, local_path))
        finally:
            session.close()
