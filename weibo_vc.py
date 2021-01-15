# !/usr/bin/env python
# --------------------------------------------------------------
# File:          weibo_vc.py
# Project:       WeiboSpider
# Created:       Friday, 15th January 2021 2:28:08 pm
# @Author:       Molin Liu, MSc in Data Science, University of Glasgow
# Contact:       molin@live.cn
# Last Modified: Friday, 15th January 2021 2:28:12 pm
# Copyright  © Rockface 2019 - 2021
# --------------------------------------------------------------
# Version control for weibos

import os, hashlib, zlib, enum
import logging

logger = logging.getLogger(__name__)

data_dir = "data"

class ObjectType(enum.Enum):
    """Object type enum. There are other types too, but we don't need them.
    See "enum object_type" in git's source (git/cache.h).
    """
    commit = 1
    tree = 2
    blob = 3

class Weibo_VersionControl:
	def __init__(self, uid) -> None:
		self.uid = uid
		self.initRepo()
	

	def initRepo(self):
		weibo_dir = os.path.join(data_dir, self.uid) 
		self.repo = weibo_dir
		if not os.path.exists(weibo_dir):
			os.mkdir(weibo_dir)
			os.mkdir(os.path.join(weibo_dir, "img"))
			os.mkdir(os.path.join(weibo_dir, "profile"))
			os.mkdir(os.path.join(weibo_dir, "versions"))
			for name in ['objects', 'refs', 'refs/heads']:
				os.mkdir(os.path.join(weibo_dir, 'versions', name))
			write_file(os.path.join(weibo_dir, 'versions', 'HEAD'),
					b'ref: refs/heads/master')
			logger.info("Initialize Empty Repository: %s"%self.uid)

	def hash_object(self, data, obj_type, write=True):
		header = '{} {}'.format(obj_type, len(data)).encode()
		full_data = header + b'\x00' + data
		sha1 = hashlib.sha1(full_data).hexdigest()
		if write:
			path = os.path.join(self.repo, 'objects', sha1[:2], sha1[2:])
			if not os.path.exists(path):
				os.makedirs(os.path.dirname(path), exist_ok=True)
				write_file(path, zlib.compress(full_data))
		return sha1

def read_file(path):
    """Read contents of file at given path as bytes."""
    with open(path, 'rb') as f:
        return f.read()


def write_file(path, data):
    """Write data bytes to file at given path."""
    with open(path, 'wb') as f:
        f.write(data)
