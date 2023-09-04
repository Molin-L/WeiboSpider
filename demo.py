import pandas as pd
import numpy as np
import weibo_spider_helper

posts = weibo_spider_helper.load_csv('data/posts.csv').iloc[:, 1:]
print(posts.head())