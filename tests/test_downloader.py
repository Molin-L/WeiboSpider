# !/usr/bin/env python
# --------------------------------------------------------------
# File:          test_downloader.py
# Project:       lymo
# Created:       Saturday, 16th January 2021 1:14:16 pm
# @Author:       Molin Liu, MSc in Data Science, University of Glasgow
# Contact:       molin@live.cn
# Last Modified: Saturday, 16th January 2021 1:14:22 pm
# Copyright  © Rockface 2019 - 2021
# --------------------------------------------------------------

import pytest
import downloader

def test_concurrent_download():
	dl = downloader.Downloader()
	urls = ["https://wx1.sinaimg.cn/orj480/006JzvuIly8gmeymt4ggaj30u00u0769.jpg", "https://tva1.sinaimg.cn/crop.0.0.640.640.640/9d44112bjw1f1xl1c10tuj20hs0hs0tw.jpg"]
	dl.download_files(urls, "data/6170194660/img")