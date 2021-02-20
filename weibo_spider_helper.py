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
from requests.api import post
from downloader import Downloader
from collections import OrderedDict
from datetime import datetime, timedelta
import traceback, json, requests, sqlite3, os, logging, sys, time
from cache import Post
from lxml import etree
import pandas as pd

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

		self.uid = uid
		# Create user profile table
		query = "create table if not exists usr_%s( id INTEGER PRIMARY KEY AUTOINCREMENT, screen_name TEXT, avator_img BLOB, description TEXT, cover_img BLOB, gender TEXT, verified TEXT, verified_type TEXT, vip TEXT, avator_link TEXT, cover_link TEXT, update_date datetime default (datetime('now', 'localtime')));"%uid
		self.cursor.execute(query)

		# Create table for user posts
		query = "create table if not exists posts_%s( id INTEGER PRIMARY KEY AUTOINCREMENT, post_content TEXT, is_repost TEXT, images BLOB, like_num REAL, repost_num REAL, comment_num REAL, comment_link TEXT, image_link TEXT, createdate datetime default (datetime('now', 'localtime')));"%uid
		self.cursor.execute(query)

		self.stage_dir = os.path.join('data', self.uid, 'stage')

	def updateProfile(self, info, img_path, uid):
		logger.info("User %s's profile has been saved to file: %s"%(uid, os.path.join(profile_dir, 'profile.csv')))
		avator_img = self._convertToBinaryData(filename=img_path[0])
		cover_img =  self._convertToBinaryData(filename=img_path[1])
		data_tuple = (info['screen_name'], avator_img, info['description'], cover_img, info['gender'], info['verified'], info['verified_type'], info['close_blue_v'], img_path[0], img_path[1])
		
		# self.updateProfile_db(uid, data_tuple)
	def updateProfile_db(self, uid, data_tuple):

		# Update database
		query = """insert into usr_%s 
		(screen_name, avator_img, description, cover_img, gender, verified, verified_type, vip, avator_link, cover_link) 
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""%uid
		self.cursor.execute(query, data_tuple)
		self.conn.commit()
		logger.info("User %s's profile has been updated"%uid)
	def insertPost(self, post:Post, uid):
		pass

	def dropTable(self, name):
		query = "drop table %s"%name
		self.cursor.execute(query)

	def _convertToBinaryData(self, filename):
		#Convert digital data to binary format
		with open(filename, 'rb') as file:
			blobData = file.read()
		return blobData

class CacheHandler:

	def __init__(self) -> None:
		self.cache_dir = os.path.join(os.path.curdir, 'data')
	
	def saveProfile(self, info, uid):
		"""
		Save Profile to workspace.

		Args:
			info ([type]): [description]
			uid ([type]): [description]

		Returns:
			[DataFrame]: DataFrame of user's profile. 
			[List]: A list of downloaded images. 
		"""		
		# Directory
		user_dir = os.path.join(self.cache_dir, uid)
		workspace_dir = os.path.join(user_dir, 'workspace')

		# Create folder for profile.
		profile_dir = os.path.join(workspace_dir, 'profile')
		img_folder = os.path.join(profile_dir, 'img')
		if not os.path.exists(img_folder):
			os.makedirs(img_folder, exist_ok=True)
		
		avator_url = info['avatar_hd']
		cover_url = info['cover_image_phone']
		
		# Download image
		dl = Downloader()
		img_path = dl.download_files([avator_url, cover_url], img_folder)
		# Save profile to disk.
		profile_dict = dict()
		profile_dict['id'] = info['id']
		profile_dict['screen_name'] = info['screen_name']
		profile_dict['description'] = info['description']
		profile_dict['gender'] = info['gender']
		profile_dict['verified'] = info['verified']
		profile_dict['verified_type'] = info['verified_type']
		profile_dict['close_blue_v'] = info['close_blue_v']
		profile_dict['followers_count'] = info['followers_count']
		profile_dict['follow_count'] = info['follow_count']
		profile_dict['cover_image_phone'] = info['cover_image_phone']
		profile_dict['avatar_hd'] = info['avatar_hd']
		
		profile_df = pd.DataFrame.from_dict([profile_dict])
		profile_df.to_csv(os.path.join(profile_dir, 'profile.csv'))
		return img_path
		
def _get_json(params):
	url = 'https://m.weibo.cn/api/container/getIndex?'
	r = requests.get(url, params=params)
	return r.json()

def get_userinfo(uid):
	"""
	Get user's profile using m.weibo.cn/api.

	Args:
		uid (str): User id from Weibo.

	Returns:
		Json: User's profile.
	"""	
	params = {'containerid': '100505' + str(uid)}
	js = _get_json(params)
	
	if js['ok']:
		info = js['data']['userInfo']
		if info.get('toolbar_menus'):
			del info['toolbar_menus']
		return info

def read_weibo_page(param):
	"""获取一页的全部微博"""
	uid, page = param
	try:
		# Get page
		logger.info("Get user: %s, page: %s"%(uid, page))
		params = {'containerid': '107603' + str(uid), 'page': page}
		js = _get_json(params)
		logger.info(js)
		# Read json response
		if js['ok']:
			weibos = js['data']['cards']
			posts = []
			for w in weibos:
				if w['card_type'] == 9:
					wb, retweet = read_post(w)
					posts.append(wb)
					if retweet!=None:
						posts.append(retweet)
			return (posts, page)
	except Exception as e:
		print("Error: ", e)
		traceback.print_exc()

def read_post(info):
	"""获取一条微博的全部信息"""
	weibo_info = info['mblog']
	weibo_id = weibo_info['id']
	retweeted_status = weibo_info.get('retweeted_status')
	is_long = weibo_info['isLongText']
	weibo = parse_weibo(weibo_info)
	if retweeted_status:  # 转发
		retweet_id = retweeted_status['id']

		if retweeted_status['text']!="抱歉，作者已设置仅展示半年内微博，此微博已不可见。 ":
			is_long_retweet = retweeted_status['isLongText']
			if is_long:
				weibo = read_long_weibo(weibo_id)
			else:
				weibo = parse_weibo(weibo_info)
			if is_long_retweet:
				retweet = read_long_weibo(retweet_id)
			else:
				retweet = parse_weibo(retweeted_status)
			retweet['created_at'] = standardize_date(
				retweeted_status['created_at'])
			return weibo, retweet
	else:  # 原创
		if is_long:
			weibo = read_long_weibo(weibo_id)
		else:
			weibo = parse_weibo(weibo_info)
	weibo['created_at'] = standardize_date(weibo_info['created_at'])
	return weibo, None

def read_long_weibo(post_id):
	"""获取长微博"""
	url = 'https://m.weibo.cn/detail/%s' % post_id
	html = requests.get(url).text
	html = html[html.find('"status":'):]
	html = html[:html.rfind('"hotScheme"')]
	html = html[:html.rfind(',')]
	html = '{' + html + '}'
	js = json.loads(html, strict=False)
	weibo_info = js['status']
	weibo = parse_weibo(weibo_info)
	return weibo


def standardize_info(weibo):
	"""标准化信息，去除乱码"""
	for k, v in weibo.items():
		if 'int' not in str(type(v)) and 'long' not in str(
				type(v)) and 'bool' not in str(type(v)):
			weibo[k] = v.replace(u"\u200b", "").encode(
				sys.stdout.encoding, "ignore").decode(sys.stdout.encoding)
	return weibo

def standardize_date(created_at):
	"""标准化微博发布时间"""
	if u"刚刚" in created_at:
		created_at = datetime.now().strftime("%Y-%m-%d")
	elif u"分钟" in created_at:
		minute = created_at[:created_at.find(u"分钟")]
		minute = timedelta(minutes=int(minute))
		created_at = (datetime.now() - minute).strftime("%Y-%m-%d")
	elif u"小时" in created_at:
		hour = created_at[:created_at.find(u"小时")]
		hour = timedelta(hours=int(hour))
		created_at = (datetime.now() - hour).strftime("%Y-%m-%d")
	elif u"昨天" in created_at:
		day = timedelta(days=1)
		created_at = (datetime.now() - day).strftime("%Y-%m-%d")
	elif created_at.count('-') == 1:
		year = datetime.now().strftime("%Y")
		created_at = year + "-" + created_at
	return created_at

def parse_weibo(weibo_info):
	weibo = OrderedDict()
	weibo['user_id'] = weibo_info['user']['id']
	weibo['screen_name'] = weibo_info['user']['screen_name']
	weibo['id'] = int(weibo_info['id'])
	text_body = weibo_info['text']
	selector = etree.HTML(text_body)
	weibo['post_content'] = etree.HTML(text_body).xpath('string(.)')
	weibo['pics'] = get_pics(weibo_info)
	weibo['location'] = get_location(selector)
	weibo['created_at'] = weibo_info['created_at']
	weibo['device'] = weibo_info['source']
	weibo['like_count'] = string_to_int(
		weibo_info['attitudes_count'])
	weibo['comments_count'] = string_to_int(
		weibo_info['comments_count'])
	weibo['reposts_count'] = string_to_int(
		weibo_info['reposts_count'])
	weibo['at_users'] = get_at_users(selector)
	weibo['post_id'] = weibo_info['id']
	return standardize_info(weibo)

def get_pics(weibo_info):
	"""获取微博原始图片url"""
	if weibo_info.get('pics'):
		pic_info = weibo_info['pics']
		pic_list = [pic['large']['url'] for pic in pic_info]
		pics = ','.join(pic_list)
	else:
		pics = ''
	return pics

def get_location(selector):
	"""获取微博发布位置"""
	location_icon = 'timeline_card_small_location_default.png'
	span_list = selector.xpath('//span')
	location = ''
	for i, span in enumerate(span_list):
		if span.xpath('img/@src'):
			if location_icon in span.xpath('img/@src')[0]:
				location = span_list[i + 1].xpath('string(.)')
				break
	return location

def get_at_users(selector):
	"""获取@用户"""
	a_list = selector.xpath('//a')
	at_users = ''
	at_list = []
	for a in a_list:
		if '@' + a.xpath('@href')[0][3:] == a.xpath('string(.)'):
			at_list.append(a.xpath('string(.)')[1:])
	if at_list:
		at_users = ','.join(at_list)
	return at_users

def string_to_int(string):
	"""字符串转换为整数"""
	if isinstance(string, int):
		return string
	elif string.endswith(u'万+'):
		string = int(string[:-2] + '0000')
	elif string.endswith(u'万'):
		string = int(string[:-1] + '0000')
	return int(string)
def gettimestr():
    t = time.time()
    timestr = str(int(round(t * 1000)))
    return timestr

def get_cookie():
	with open("cookies.json","r") as f:
			cookies = json.load(f)
	return cookies

def like_wb(cookie, post_id):
	s = requests.session()
	s.keep_alive = False
	postheader = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36','Content-Type': 'application/x-www-form-urlencoded; Charset=UTF-8','Accept': '*/*','Accept-Language': 'zh-cn','Referer': 'https://weibo.com/'}
	
	mid = post_id
	posttime = gettimestr()
	posturl = "https://weibo.com/aj/v6/like/add?ajwvr=6&__rnd=" + posttime
	postdata = "location=page_100206_single_weibo&version=mini&qid=heart&mid=" + mid + "&loc=profile&cuslike=1&hide_multi_attitude=1&liked=0&_t=0"
	response = s.post(posturl,data=postdata,headers=postheader,cookies=cookie,timeout=5)
	code = response.json()['code']
	if (code == '100000'):
		print("点赞成功")
	else:
		print("点赞失败")
if __name__ == "__main__":
	uid = "3196393410"
	print(read_weibo_page((uid, 1)))
