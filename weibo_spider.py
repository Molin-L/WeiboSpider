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

import tqdm
from logger import logger

class WeiboSpider:
	def __init__(self) -> None:
		super().__init__()
		