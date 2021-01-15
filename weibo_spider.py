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

import tqdm, os
import logging, datetime
import weibo_vc, weibo_spider_helper

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
		self.db.updateProfile(user_profile, self.uid)
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