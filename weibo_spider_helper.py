# !/usr/bin/env python
# --------------------------------------------------------------
# File:          weibo_spider_helper.py
# Project:       WeiboSpider
# Created:       Friday, 15th January 2021 1:49:38 pm
# @Author:       Molin Liu, MSc in Data Science, University of Glasgow
# Contact:       molin@live.cn
# Last Modified: Friday, 15th January 2021 1:50:50 pm
# Copyright  © Rockface 2019 - 2021
# --------------------------------------------------------------
from downloader import Downloader
import requests, sqlite3, os
import logging
from cache import Post

logger = logging.getLogger(__name__)
class DBConnect:

	def __init__(self) -> None:
		super().__init__()
		self.db_path = os.path.join('data', 'base_data.db')
		self.connectDB()
	
	def connectDB(self):
		self.conn = sqlite3.connect(self.db_path)
		logger.info("Success connect db.")
		self.cursor = self.conn.cursor()

	def initUser(self, uid):
		# Create table for user posts
		query = "create table if not exists posts_%s( id INTEGER PRIMARY KEY AUTOINCREMENT, post_content TEXT, images BLOB, like_num REAL, repost_num REAL, comment_num REAL, comment_link TEXT, image_link TEXT, createdate datetime default (datetime('now', 'localtime')));"%uid
		self.cursor.execute(query)

		# Create user profile table
		query = "create table if not exists usr_%s( id INTEGER PRIMARY KEY AUTOINCREMENT, screen_name TEXT, avator_img BLOB, description TEXT, cover_img BLOB, gender TEXT, verified TEXT, verified_type TEXT, vip TEXT, avator_link TEXT, cover_link TEXT, update_date datetime default (datetime('now', 'localtime')));"%uid
		self.cursor.execute(query)
	
	def updateProfile(self, info, uid):
		avator_url = info['avatar_hd']
		cover_url = info['cover_image_phone']
		img_folder = os.path.join('data', uid, 'img')
		dl = Downloader()
		img_path = dl.download_files([avator_url, cover_url], img_folder)

		query = """insert into usr_%s 
		(screen_name, avator_img, description, cover_img, gender, verified, verified_type, vip, avator_link, cover_link) 
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""%uid

		avator_img = self._convertToBinaryData(filename=img_path[0])
		cover_img =  self._convertToBinaryData(filename=img_path[1])
		data_tuple = (info['screen_name'], avator_img, info['description'], cover_img, info['gender'], info['verified'], info['verified_type'], info['close_blue_v'], img_path[0], img_path[1])
		self.cursor.execute(query, data_tuple)
		self.conn.commit()
		logger.info("User %s's profile has been updated"%uid)
	
	def insertPost(self, post:Post, uid):


	def dropTable(self, name):
		query = "drop table %s"%name
		self.cursor.execute(query)

	def _convertToBinaryData(self, filename):
		#Convert digital data to binary format
		with open(filename, 'rb') as file:
			blobData = file.read()
		return blobData

def _get_json(params):
	url = 'https://m.weibo.cn/api/container/getIndex?'
	r = requests.get(url, params=params)
	return r.json()

def get_userinfo(uid):
	params = {'containerid': '100505' + str(uid)}
	js = _get_json(params)
	if js['ok']:
		info = js['data']['userInfo']
		if info.get('toolbar_menus'):
			del info['toolbar_menus']
		return info

if __name__ == "__main__":
	get_userinfo("6170194660")
