# !/usr/bin/env python
# --------------------------------------------------------------
# File:          weibo_spider.py
# Project:       lymo
# Created:       Friday, 15th January 2021 1:38:50 pm
# @Author:       Molin Liu, MSc in Data Science, University of Glasgow
# Contact:       molin@live.cn
# Last Modified: Friday, 15th January 2021 1:38:53 pm
# Copyright  © Rockface 2019 - 2020
# --------------------------------------------------------------

from multiprocessing import Pool, cpu_count
import tqdm, os, math
import logging, datetime
import weibo_vc, weibo_spider_helper
import pandas as pd

logger = logging.getLogger(__name__)
class SingleWeiboSpider():

	def __init__(self, uid):
	
		self.uid = uid
		# Initialize version control for a single user.
		self.vc = weibo_vc.Weibo_VersionControl(uid)

		# Connect to and initialize local database
		self.data_path = os.path.join('data', uid)
		self.db = weibo_spider_helper.DBConnect()
		self.db.initUser(self.uid)
	
	def update_userprofile(self):
		user_profile = weibo_spider_helper.get_userinfo(self.uid)
		self.user_profile = user_profile
		# Calculate pages number
		self.posts_num = user_profile['statuses_count']
		self.pages_num = int(math.ceil(self.posts_num / 10.0))

		# Update database
		self.db.updateProfile(user_profile, self.uid)

	def update_post(self):
		# Concurrent get
		try:
			workers = cpu_count()
		except NotImplementedError:
			workers = 1
		pool = Pool(processes=workers)
		pages = list(range(1, self.pages_num+1))
		posts = []
		for posts_page in tqdm.tqdm(pool.imap_unordered(weibo_spider_helper.read_weibo_page, zip([self.uid]*self.pages_num, pages)), total=len(pages)):
			posts.append(posts_page)
		pool.close()
		pool.join()

		posts.sort(key=lambda x: x[1])
		all_post = []
		for posts_page in posts:
			all_post += posts_page[0]
		df = pd.DataFrame.from_dict(all_post)
		df.to_csv(os.path.join(self.data_path, 'stage', 'posts.csv'))
		
class WeiboSpider:
	def __init__(self) -> None:
		super().__init__()

if __name__ == "__main__":
	log_folder = 'Log'
	if not os.path.exists(log_folder):
		os.mkdir(log_folder)

	today = datetime.date.today()
	time = today.strftime("%Y-%m-%d")
	logging.basicConfig(
		level=logging.INFO, 
		filename="Log/"+time+".log",
		filemode='a',
		format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	job = SingleWeiboSpider("6170194660")
	job.update_userprofile()
	job.update_post()