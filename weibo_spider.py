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
from requests.api import post
import tqdm, os, math
import logging, datetime
import weibo_vc, weibo_spider_helper
import pandas as pd

logger = logging.getLogger(__name__)
class SingleWeiboSpider():

	def __init__(self, uid):
	
		self.uid = uid
		# Initialize version control for a single user.
		self.vc = weibo_vc.VersionControl(uid)

		# Connect to and initialize local database
		self.data_path = os.path.join('data', uid)
		self.db = weibo_spider_helper.DBConnect("base_data")
		self.db.initUser(self.uid)
		self.cache_handler = weibo_spider_helper.CacheHandler(self.uid)
		self.update_userprofile()
	
	def update_userprofile(self):
		user_profile = weibo_spider_helper.get_userinfo(self.uid)
		self.user_profile = user_profile
		posts, page = weibo_spider_helper.read_weibo_page((self.uid, 1))
		# 5669280306
		# first_post = posts[0]['post_id']
		# weibo_spider_helper.like_wb(weibo_spider_helper.get_cookie(), first_post)
		# Calculate pages number
		self.posts_num = user_profile['statuses_count']
		self.pages_num = int(math.ceil(self.posts_num / 10.0))

		# Save to local files and database.
		self._save_profile(user_profile)

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

		posts = [i for i in posts if i!=None]

		posts.sort(key=lambda x: x[1])
		all_post = []
		for posts_page in posts:
			all_post += posts_page[0]
		downloaded_df = pd.DataFrame.from_dict(all_post)
		downloaded_df.to_csv(os.path.join(self.data_path, 'workspace', 'posts', "raw_posts.csv"), index=False)
		result_df = self._process_posts(downloaded_df)
		self._save_posts(result_df)
	
	def load_csv(self, path):
		csv_df = weibo_spider_helper.load_csv(path).iloc[:, 1:]
		result_df = self._process_posts(csv_df)
		self._save_posts(result_df)

	def _process_posts(self, posts_df:pd.DataFrame):
		"""
		Mark deleted posts and get the new posts.

		Args:
			posts_df (pd.DataFrame): the original posts.
		"""
		posts_df['is_deleted'] = len(posts_df)*['']
		posts_df['deleted_timestamp'] = len(posts_df)*['']
		posts_df['local_imgs'] = len(posts_df)*['']
		posts_df = weibo_spider_helper.format_df(posts_df)
		# Mark posts' status
		local_posts_path = os.path.join(self.data_path, 'workspace', 'posts', 'posts.csv')
		if os.path.exists(local_posts_path):
			local_posts_df = pd.read_csv(local_posts_path)
			result_df, new_df = weibo_spider_helper.tag_posts(posts_df, local_posts_df)
		else:
			result_df = posts_df
			new_df = result_df
		new_df = weibo_spider_helper.download_posts_img(self.uid, new_df)

		return result_df
		
		
	def _save_posts(self, posts_df:pd.DataFrame):
		posts_df.to_csv(os.path.join(self.data_path, 'workspace', 'posts', 'posts.csv'), index=False)
		if self.vc.commit('posts'):
			self.db.update_posts(self.uid, posts_df)
	
	def _save_profile(self, profile_info: dict, mode="release"):
		"""
		Downloads avator and cover. Commit version control. Update database.
		"""
		self.cache_handler.saveProfile(profile_info)

		avator_url = profile_info['avatar_hd']
		cover_url = profile_info['cover_image_phone']
		path = [avator_url, cover_url]
		
		if mode=='test':
			img_path = [
				"data/t_3196393410/workspace/profile/img/9d44112bjw1f1xl1c10tuj20hs0hs0tw.jpg",
				"data/t_3196393410/workspace/profile/img/be8517c2ly8gkiw5bvqf1j20mk0mkq3q.jpg"
				]
		elif mode=='release':
			img_path = self.cache_handler.download_imgs(path, os.path.join(self.data_path, 'workspace', 'profile'))
		else:
			raise NotImplementedError
			img_path = []

		if self.vc.commit('profile'):
			self.db.updateProfile(profile_info, img_path, self.uid)
		
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
	job = SingleWeiboSpider("5669280306")
	job.update_userprofile()
	job.update_post()