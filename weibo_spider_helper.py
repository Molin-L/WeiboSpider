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
import requests
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
		#user_info = self.__standardize_info(info)
		#self.user = user_info
		#return user_info
		print(info)

if __name__ == "__main__":
	get_userinfo("6170194660")
