# !/usr/bin/env python
# --------------------------------------------------------------
# File:          logger.py
# Project:       WeiboSpider
# Created:       Friday, 15th January 2021 1:41:39 pm
# @Author:       Molin Liu, MSc in Data Science, University of Glasgow
# Contact:       molin@live.cn
# Last Modified: Friday, 15th January 2021 1:42:38 pm
# Copyright  © Rockface 2019 - 2020
# --------------------------------------------------------------
import logging
import datetime
import os

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
logger = logging.getLogger(__name__)
