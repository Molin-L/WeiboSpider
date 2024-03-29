# !/usr/bin/env python
# --------------------------------------------------------------
# File:          weibo.py
# Project:       WeiboSpider
# Created:       Friday, 15th January 2021 1:59:29 pm
# @Author:       Molin Liu, MSc in Data Science, University of Glasgow
# Contact:       molin@live.cn
# Last Modified: Friday, 15th January 2021 2:02:15 pm
# Copyright  © Rockface 2019 - 2021
# --------------------------------------------------------------

from typing import OrderedDict


class Post:
    def __init__(self):
        self.info = OrderedDict()
        self.info['screen_name'] = ''
        self.id = ''
        self.user_id = ''

        self.content = ''
        self.article_url = ''

        self.original_pictures = []
        self.retweet_pictures = None
        self.original = None
        self.video_url = ''

        self.publish_place = ''
        self.publish_time = ''
        self.publish_tool = ''

        self.up_num = 0
        self.retweet_num = 0
        self.comment_num = 0

    def __str__(self):
        """打印一条微博"""
        result = self.content + '\n'
        result += u'微博发布位置：%s\n' % self.publish_place
        result += u'发布时间：%s\n' % self.publish_time
        result += u'发布工具：%s\n' % self.publish_tool
        result += u'点赞数：%d\n' % self.up_num
        result += u'转发数：%d\n' % self.retweet_num
        result += u'评论数：%d\n' % self.comment_num
        result += u'url：https://weibo.cn/comment/%s\n' % self.id
        return result

class Weibo:
    def __init__(self) -> None:
        self.id = '' # Screen_name
        self.user_id = ''
        self.description = ''
        self.follow_num = 0
        self.follower_num = 0
        self.gender = ''
        self.verified = ''
        self.close_blue_v = ''

        # Images
        self.avator_url = ''
        self.cover_url = ''
    
