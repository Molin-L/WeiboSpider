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
from shutil import copyfile
import shutil

test_uid = "t_5669280306"
@pytest.mark.run(order=1)
def test_init():
	test_vc = weibo_vc.VersionControl(test_uid)
	test_dir = os.path.join('data', test_uid)
	assert os.path.exists(test_dir)

@pytest.mark.run(order=2)
def test_io():
	test_vc = weibo_vc.VersionControl(test_uid)
	test_dir = os.path.join('data', test_uid)
	file_path = os.path.join(test_dir, 'workspace/profile/test.txt')
	with open(file_path, 'w') as f:
		f.write("Hello world\n")
	data = weibo_vc.read_file(file_path)
	sha1 = test_vc._hash_object(data, 'blob', True)
	_, data_read = test_vc._read_object(sha1)
	assert data_read.decode()=="Hello world\n"

def test_history_record():
	test_vc = weibo_vc.VersionControl(test_uid)
	hf = weibo_vc.HistoryRecord(test_vc.profile_history)
	hf.info("hello")
	with open(test_vc.profile_history, 'r') as f:
		data = f.read()
		assert data[-6:] == "hello\n"

@pytest.mark.run(order=3)
def test_commit():
	test_vc = weibo_vc.VersionControl(test_uid)
	test_dir = os.path.join('data', test_uid)
	test_vc.commit('profile')
	file_path = os.path.join(test_dir, 'workspace/profile/test.txt')
	with open(file_path, 'a') as f:
		f.write("This is new line.\n")
	test_vc.commit('profile')

def teardown_module():
	shutil.rmtree("data/{}".format(test_uid))