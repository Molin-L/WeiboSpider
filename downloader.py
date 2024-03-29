# !/usr/bin/env python
# --------------------------------------------------------------
# File:          downloader.py
# Project:       WeiboSpider
# Created:       Friday, 15th January 2021 5:35:35 pm
# @Author:       Molin Liu, MSc in Data Science, University of Glasgow
# Contact:       molin@live.cn
# Last Modified: Friday, 15th January 2021 5:35:37 pm
# Copyright  © Rockface 2019 - 2021
# --------------------------------------------------------------
from multiprocessing import Pool, cpu_count
from typing import List
import requests, os, logging, sys
from requests.adapters import HTTPAdapter
import tqdm

logger = logging.getLogger(__name__)

def download_one_file(params):
	url, file_path = params
	"""下载单个文件(图片/视频)"""
	damaged_folder = 'data/damaged'
	file_download_timeout = [5, 5, 10]
	try:
		if not os.path.isfile(file_path):
			s = requests.Session()
			s.mount(url,
					HTTPAdapter())
			downloaded = s.get(url)
			with open(file_path, 'wb') as f:
				f.write(downloaded.content)
	except Exception as e:
		error_file = os.path.join(damaged_folder, 'not_downloaded.txt')
		with open(error_file, 'a') as f:
			f.write(url.encode(sys.stdout.encoding))
			f.write("\n")
		logger.exception(e)
class Downloader:
	def __init__(self) -> None:
		damaged_folder = 'data/damaged'
		if not os.path.exists(damaged_folder):
			os.mkdir(damaged_folder)
	def download_files(self, urls:List, data_folder) -> List:
		try:
			workers = cpu_count()
		except NotImplementedError:
			workers = 1
		pool = Pool(processes=workers)
		if not os.path.exists(data_folder):
			os.makedirs(data_folder)
		files_path = []
		for i in range(len(urls)):
			file_path = os.path.join(data_folder, urls[i].split("/")[-1])
			files_path.append(str(file_path))
		for _ in tqdm.tqdm(pool.imap_unordered(download_one_file, zip(urls, files_path)), total=len(urls)):
			pass
		pool.close()
		pool.join()
		return files_path

if __name__ == "__main__":
	pass
	
