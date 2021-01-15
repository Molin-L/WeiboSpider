# !/usr/bin/env python
# --------------------------------------------------------------
# File:          test_vc.py
# Project:       lymo
# Created:       Friday, 15th January 2021 3:04:44 pm
# @Author:       Molin Liu, MSc in Data Science, University of Glasgow
# Contact:       molin@live.cn
# Last Modified: Friday, 15th January 2021 3:10:34 pm
# Copyright  © Rockface 2019 - 2021
# --------------------------------------------------------------
import weibo_vc
import pytest
import os

def test_vc_init():
	uid = "6170194660"
	weibo_vc.init(uid)
	assert os.path.exists("data/6170194660")