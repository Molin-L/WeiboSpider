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
from sqlite3.dbapi2 import Cursor
from pandas.core import accessor
from requests.api import post
from downloader import Downloader
from collections import OrderedDict
from datetime import datetime, timedelta
import traceback, json, requests, sqlite3, os, logging, sys, time
from cache import Post
from lxml import etree
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
datatype = {'post_id': 'string', 'user_id': 'string', 'screen_name': 'string', 'at_users': 'string', 'is_deleted': 'string', 'location': 'string', 'deleted_timestamp': 'string', 'device': 'string', 'post_content': 'string', 'pics_origin': 'string', 'local_imgs': 'string', 'created_at': 'string'}
class DBConnect:
	def __init__(self, db) -> None:
		super().__init__()
		self.db_path = os.path.join('data', f'{db}.db')
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
		
		# Migrate to pd.DataFrame.to_sql(), which makes easier to add columns.
		#
		# Create table for user posts
		#
		# query = "create table if not exists posts_%s( id INTEGER PRIMARY KEY AUTOINCREMENT, post_content TEXT, is_repost TEXT, images BLOB, like_num REAL, repost_num REAL, comment_num REAL, comment_link TEXT, image_link TEXT, createdate datetime default (datetime('now', 'localtime')));"%uid
		#self.cursor.execute(query)

		self.stage_dir = os.path.join('data', self.uid, 'stage')

	def updateProfile(self, info, img_path, uid):
		logger.info("User %s's profile has been saved to file: %s"%(uid, os.path.join('data', self.uid, 'workspace', 'profile', 'profile.csv')))
		avator_img = self._convertToBinaryData(filename=img_path[0])
		cover_img =  self._convertToBinaryData(filename=img_path[1])
		data_tuple = (info['screen_name'], avator_img, info['description'], cover_img, info['gender'], info['verified'], info['verified_type'], info['close_blue_v'], img_path[0], img_path[1])
		
		self.update_profile(uid, data_tuple)
		
	def update_profile(self, uid, data_tuple):

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

	def update_posts(self, uid, posts_df:pd.DataFrame):
		"""
		Update user's posts to database.

		Args:
			uid (str): User id provided by Weibo.
			posts_df (pandas.DataFrame): DataFrame which recodes user's posts.
		"""
		des_table = "posts_"+uid
		
		posts_df.to_sql("tmp", self.conn, if_exists='replace')
		sql = f"""
			UPDATE {des_table} set last_seen = (SELECT t.last_seen
											FROM tmp t
											WHERE t.Key = table_name.key)
			WHERE EXISTS(
				SELECT *
				FROM tmp
				WHERE tmp.key = table_name.key
			)
			"""
		self.cursor.execute("select count(*) from sqlite_master where type='table' and name='{}';".format(des_table))
		is_table = self.cursor.fetchall()[0][0]
		if  is_table != 0:
			history_df = pd.read_sql(f"select * from {des_table}", self.conn).astype(datatype)
			posts_df = pd.merge(update_local_posts(history_df, posts_df), posts_df, "outer").sort_values(by=["post_id"], ascending=False)

		posts_df.to_sql(des_table, self.conn, index=False, if_exists="replace")
class CacheHandler:

	def __init__(self, uid) -> None:
		self.cache_dir = os.path.join(os.path.curdir, 'data')
		self.uid = uid
		# Directory
		self.user_dir = os.path.join(self.cache_dir, uid)
		self.workspace_dir = os.path.join(self.user_dir, 'workspace')
		self.profile_dir = os.path.join(self.workspace_dir, 'profile')
		self.posts_dir = os.path.join(self.workspace_dir, 'posts')

	def saveProfile(self, info: dict) -> None:
		"""
		Save Profile to workspace.

		Args:
			info (dict): [description]

		"""	

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
		profile_df.to_csv(os.path.join(self.profile_dir, 'profile.csv'), index=False)
	
	def download_imgs(self, path: list, save_path):

		img_folder = os.path.join(save_path, 'img')
		if not os.path.exists(img_folder):
			os.makedirs(img_folder, exist_ok=True)
		
		# Download image
		dl = Downloader()
		img_path = dl.download_files(path, img_folder)
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
	"""
	Read posts in one page.

	Args:
		param (tuple): uid, page

	Returns:
		tuple: posts, page_number
	"""
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
	weibo['post_id'] = weibo_info['id']
	text_body = weibo_info['text']
	selector = etree.HTML(text_body)
	weibo['post_content'] = etree.HTML(text_body).xpath('string(.)')
	weibo['pics_origin'] = get_pics(weibo_info)
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

def update_posts_df(df_a:pd.DataFrame, df_b:pd.DataFrame):
	"""
	Update local data with the current new posts.

	Current pd.DataFrame.update() has some bug, which may cause losing data and will not preserve dtypes.

	However this method will not update the content of posts. 
	Args:
		current_posts (pd.DataFrame): New posts just downloaded from servers.
		local_posts (pd.DataFrame): Local posts stored in data folder.

	Returns:
		pd.DataFrame: [description]
	"""
	result = df_a.set_index("post_id")
	result.update(df_b.set_index("post_id"))
	result.reset_index(drop=False, inplace=True)
	return result
def download_posts_img(uid, posts:pd.DataFrame):
	imgs = []
	temp = posts[posts['pics_origin'].ne('')]['pics_origin'].to_list()
	for i in temp:
		if isinstance(i, str):
			imgs+=i.split(',')
	dl = Downloader()
	print(imgs)
	logger.info(f"Downloading {len(temp)} images")
	local_path = dl.download_files(imgs, os.path.join('data', uid, 'workspace', 'posts', 'img'))
	
	posts = convert_imgs_path(dict(zip(imgs, local_path)), posts)
	# posts.to_csv(os.path.join('data', uid, 'workspace', 'posts', 'temp_posts.csv'), index=False)
	return posts

def convert_imgs_path(convert_path, posts:pd.DataFrame):
	posts['local_imgs'] = len(posts)*['']
	
	for index, row in posts.iterrows():
		if isinstance(row['pics_origin'], str):
			if row['pics_origin']!='':
				temp_path = row['pics_origin'].split(',')
				local_path = map(convert_path.get, temp_path)
				posts.loc[index,'local_imgs'] = ','.join(local_path)
	return posts

def tag_posts(current_df:pd.DataFrame, local_df:pd.DataFrame):
	"""
	Mark those posts which have been deleted.
	The result will be saved under 'is_deleted' and 'deleted_timestamp' columns.

	Args:
		current_df (pd.DataFrame): [description]
		local_df (pd.DataFrame): [description]

	Returns:
		pd.DataFrame: marked dataframe.
	"""
	local_df_id = set(local_df['post_id'])
	current_df_id = set(current_df['post_id'])
	new_id = current_df_id-local_df_id
	delete_id = local_df_id-current_df_id
	# Find the differential posts id
	print("New posts: {}".format(str(new_id)))
	print("Deleted posts: {}".format(str(delete_id)))
	
	new_posts = current_df[current_df['post_id'].isin(new_id)]

	# Merge current posts and new posts.
	#result = old_df.merge(current_df, how='outer')
	result = local_df.combine_first(current_df)
	result = result.merge(new_posts, how='outer')
	# Create is_deleted and deleted_timestamp columns
	timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	result.loc[result['post_id'].isin(delete_id), ['is_deleted', 'deleted_timestamp']] = ['yes', timestamp]
	result = result.astype({'post_id':'string'})
	result.sort_values(by='post_id', inplace=True, ascending=False)

	
	return result, new_posts

def update_local_posts(current_posts: pd.DataFrame, local_posts: pd.DataFrame) -> pd.DataFrame:
	"""
	Update local data with the current new posts.

	Current pd.DataFrame.update() has some bug, which may cause losing data and will not preserve dtypes.

	However this method will not update the content of posts. 
	Args:
		current_posts (pd.DataFrame): New posts just downloaded from servers.
		local_posts (pd.DataFrame): Local posts stored in data folder.

	Returns:
		pd.DataFrame: [description]
	"""
	local_df_id = set(local_posts['post_id'])
	current_df_id = set(current_posts['post_id'])
	new_id = current_df_id-local_df_id
	delete_id = local_df_id-current_df_id
	# Find the differential posts id
	print("New posts: {}".format(str(new_id)))
	print("Deleted posts: {}".format(str(delete_id)))
	
	new_posts = current_posts[current_posts['post_id'].isin(new_id)]
	print(new_posts)
	# Merge current posts and new posts.
	print(current_posts.dtypes)
	result = local_posts.combine_first(current_posts.astype(datatype)).astype(datatype)
	new_order = ["user_id","screen_name","post_id","post_content","pics_origin","location","created_at","device","like_count","comments_count","reposts_count","at_users"]

	#result.append(new_posts)
	result.sort_values(by='post_id', inplace=True, ascending=False)
	result = result.reindex(new_order, axis=1)
	return result

def format_df(df:pd.DataFrame)->pd.DataFrame:

	df.sort_values(by='post_id', inplace=True)
	return df.astype(datatype)

def load_csv(path) ->pd.DataFrame:

	tmp_df = pd.read_csv(path, dtype=datatype)
	return tmp_df

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
	posts = pd.read_csv('data/testset/posts.csv')
	db = DBConnect("test_db")
	db.update_posts('test_user', posts)