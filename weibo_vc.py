# !/usr/bin/env python
# --------------------------------------------------------------
# File:          weibo_vc.py
# Project:       WeiboSpider
# Created:       Friday, 15th January 2021 2:28:08 pm
# @Author:       Molin Liu, MSc in Data Science, University of Glasgow
# Contact:       molin@live.cn
# Last Modified: Friday, 15th January 2021 2:28:12 pm
# Copyright  © Rockface 2019 - 2021
# --------------------------------------------------------------
# Version control for weibos

import os, hashlib, zlib, enum, collections, difflib, struct, datetime, time
import logging

logger = logging.getLogger(__name__)
IndexEntry = collections.namedtuple('IndexEntry', [
	'ctime_s', 'ctime_n', 'mtime_s', 'mtime_n', 'dev', 'ino', 'mode', 'uid',
	'gid', 'size', 'sha1', 'flags', 'path',
])

class HistoryRecord:

	def __init__(self, history_path) -> None:
		self.history_path = history_path

	def info(self, info_str:str):
		time_info = datetime.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S %z")
		
		msg = []
		msg.append(time_info)
		msg.append("INFO")
		msg.append(info_str)
		msg = " - ".join(msg)+"\n"
		with open(self.history_path, 'a') as f:
			f.write(msg)

class VersionControl:
	def __init__(self, uid, data_dir="data") -> None:
		self.uid = uid
		weibo_dir = os.path.join(data_dir, self.uid) 
		self.repo = weibo_dir
		if not os.path.exists(weibo_dir):
			os.mkdir(weibo_dir)
			os.mkdir(os.path.join(weibo_dir, "versions"))
			os.mkdir(os.path.join(weibo_dir, "workspace"))
			os.mkdir(os.path.join(weibo_dir, "stage"))
			for name in ['objects', 'refs', 'refs/heads']:
				os.mkdir(os.path.join(weibo_dir, 'versions', name))
			for name in ['posts', 'profile']:
				os.mkdir(os.path.join(weibo_dir, 'workspace', name))
			logger.info("Initialize Empty Repository: %s"%self.uid)
		self.profile_dir = os.path.join(self.repo, 'workspace', 'profile')
		self.posts_dir = os.path.join(self.repo, 'workspace', 'posts')
		self.profile_index = os.path.join(self.repo, "ProfileIndex")
		self.posts_index = os.path.join(self.repo, "PostsIndex")

		self.posts_history = os.path.join(self.repo, 'posts_history.log')
		self.profile_history = os.path.join(self.repo, 'profile_history.log')
		self.master_profile = os.path.join(self.repo, 'versions', 'refs', 'heads', 'profile_master')
		self.master_posts = os.path.join(self.repo, 'versions', 'refs', 'heads', 'posts_master')

	def commit(self, mode='profile'):

		changed, newfile, delete = self._get_status(mode)
		if (len(changed)+len(newfile)+len(delete))<1:
			logger.info("Nothing to commit.")
			return False
		
		self._add_file(mode)
		self._create_commit(mode)
		return True

	def commit_history(self, commit_sha1):
		obj_type, commit = self._read_object(commit_sha1)
		assert obj_type == 'commit'
		lines = commit.decode().splitlines()
		parents = (l[7:47] for l in lines if l.startswith('parent '))

		commit_history = []
		commit_history.append(commit_sha1)
		for parent in parents:
			commit_history+=self.commit_history(parent)
		return commit_history
	
	def read_commit(self, commit_sha1):
		obj_type, commit = self._read_object(commit_sha1)
		assert obj_type == 'commit'
		lines = commit.decode().splitlines()
		index_sha1 = (l[6:] for l in lines if l.startswith('index '))
		for sha1 in index_sha1:
			_, index_file = self._read_object(sha1)
			entries = read_index_data(data=index_file)
			return entries
	def compare_commit(self, sha1_x, sha1_y):
		entries_x = self.read_commit(sha1_x)
		entries_y = self.read_commit(sha1_y)
		obj_type, data1 = self.read_object(entries_x[0].sha1.hex())
		print(data1.decode())

	def _create_commit(self, mode):

		parent = self._get_parent(mode)
		timestamp = int(time.mktime(time.localtime()))
		utc_offset = -time.timezone
		author = "Weibo Spider"
		author_time = '{} {}{:02}{:02}'.format(
				timestamp,
				'+' if utc_offset > 0 else '-',
				abs(utc_offset) // 3600,
				(abs(utc_offset) // 60) % 60)
		lines = []
		if parent:
			lines.append('parent ' + parent)
		lines.append('author {} {}'.format(author, author_time))
		lines.append('committer {} {}'.format(author, author_time))
		if mode=='profile':
			index_data = read_file(self.profile_index)
		else:
			index_data = read_file(self.posts_index)

		index_sha1 = self._hash_object(index_data, 'index')
		lines.append('index {}'.format(index_sha1))
		data = '\n'.join(lines).encode()

		sha1 = self._hash_object(data, 'commit')

		if mode=='posts':
			master_path = self.master_posts
		else:
			master_path = self.master_profile
		write_file(master_path, (sha1+'\n').encode())
		logger.info("Commit to {} master: {}".format(mode, sha1))
		return sha1

	def _add_file(self, mode="profile"):
		"""
		Write ProfileIndex.
		"""
		paths = set()
		if mode=='profile':
			workspace = self.profile_dir
			outpath = self.profile_index
		else:
			workspace = self.posts_dir
			outpath = self.posts_index
		for root, dirs, files in os.walk(workspace):
			dirs[:] = [d for d in dirs if d != '.git']
			for file in files:
				path = os.path.join(root, file)
				path = path.replace('\\', '/')
				if path.startswith('./'):
					path = path[2:]
				paths.add(path)
		entries = self._generate_entries(paths)
		write_file(outpath, self._generate_index(entries))
		
	def _hash_object(self, data, obj_type, write=True):
		header = '{} {}'.format(obj_type, len(data)).encode()
		full_data = header + b'\x00' + data
		sha1 = hashlib.sha1(full_data).hexdigest()
		if write:
			path = os.path.join(self.repo, 'versions', 'objects', sha1[:2], sha1[2:])
			if not os.path.exists(path):
				os.makedirs(os.path.dirname(path), exist_ok=True)
				write_file(path, zlib.compress(full_data))
		return sha1

	def _read_object(self, sha1_prefix):
		"""Read object with given SHA-1 prefix and return tuple of
		(object_type, data_bytes), or raise ValueError if not found.
		"""
		path = self._find_object(sha1_prefix)
		full_data = zlib.decompress(read_file(path))
		nul_index = full_data.index(b'\x00')
		header = full_data[:nul_index]
		obj_type, size_str = header.decode().split()
		size = int(size_str)
		data = full_data[nul_index + 1:]
		assert size == len(data), 'expected size {}, got {} bytes'.format(
				size, len(data))
		return (obj_type, data)

	def _find_object(self, sha1_prefix):
		"""Find object with given SHA-1 prefix and return path to object in object
		store, or raise ValueError if there are no objects or multiple objects
		with this prefix.
		"""
		if len(sha1_prefix) < 2:
			raise ValueError('hash prefix must be 2 or more characters')
		obj_dir = os.path.join(self.repo, 'versions/objects', sha1_prefix[:2])
		rest = sha1_prefix[2:]
		objects = [name for name in os.listdir(obj_dir) if name.startswith(rest)]
		if not objects:
			raise ValueError('object {!r} not found'.format(sha1_prefix))
		if len(objects) >= 2:
			raise ValueError('multiple objects ({}) with prefix {!r}'.format(
					len(objects), sha1_prefix))
		return os.path.join(obj_dir, objects[0])

	def _generate_entries(self, paths):
		entries = []
		for path in paths:
			data = read_file(path)
			sha1 = self._hash_object(data, 'blob')
			st = os.stat(path)
			flags = len(path.encode())
			assert flags < (1 << 12)
			entry = IndexEntry(
					int(st.st_ctime), 0, int(st.st_mtime), 0, st.st_dev,
					st.st_ino, st.st_mode, st.st_uid, st.st_gid, st.st_size,
					bytes.fromhex(sha1), flags, path)
			entries.append(entry)
		return entries
	def _get_parent(self, mode):
		"""Get current commit hash (SHA-1 string) of local master branch."""
		if mode=='posts':
			master_path = os.path.join(self.repo, 'versions', 'refs', 'heads', 'posts_master')
		else:
			master_path = os.path.join(self.repo, 'versions', 'refs', 'heads', 'profile_master')
		try:
			return read_file(master_path).decode().strip()
		except FileNotFoundError:
			return None
	def _generate_index(self, entries):
		packed_entries = []
		for entry in entries:
			entry_head = struct.pack('!LLLLLLLLLL20sH',
					entry.ctime_s, entry.ctime_n, entry.mtime_s, entry.mtime_n,
					entry.dev, entry.ino, entry.mode, entry.uid, entry.gid,
					entry.size, entry.sha1, entry.flags)
			path = entry.path.encode()
			length = ((62 + len(path) + 8) // 8) * 8
			packed_entry = entry_head + path + b'\x00' * (length - 62 - len(path))
			packed_entries.append(packed_entry)
		header = struct.pack('!4sLL', b'DIRC', 2, len(entries))
		all_data = header + b''.join(packed_entries)
		digest = hashlib.sha1(all_data).digest()

		return all_data+digest
	def _get_status(self, mode):
		if mode=='posts':
			index_path = self.posts_index
			workspace_dir = self.posts_dir
		else:
			index_path = self.profile_index
			workspace_dir = self.profile_dir
		paths = set()
		for root, dirs, files in os.walk(workspace_dir):
			dirs[:] = [d for d in dirs if d != '.git']
			for file in files:
				path = os.path.join(root, file)
				path = path.replace('\\', '/')
				if path.startswith('./'):
					path = path[2:]
				paths.add(path)
		entries_by_path = {e.path: e for e in read_index(index_path)}
		entry_paths = set(entries_by_path)
		changed = {p for p in (paths & entry_paths)
				if self._hash_object(read_file(p), 'blob', write=False) !=
					entries_by_path[p].sha1.hex()}
		new = paths - entry_paths
		deleted = entry_paths - paths
		return (sorted(changed), sorted(new), sorted(deleted))


def read_file(path):
	"""Read contents of file at given path as bytes."""
	with open(path, 'rb') as f:
		return f.read()


def write_file(path, data):
	"""Write data bytes to file at given path."""
	with open(path, 'wb') as f:
		f.write(data)

def read_index(path=None):
	"""Read git index file and return list of IndexEntry objects."""
	if path!=None:
		try:
			data = read_file(path)
		except FileNotFoundError:
			return []
	return read_index_data(data)

def read_index_data(data):

	digest = hashlib.sha1(data[:-20]).digest()
	assert digest == data[-20:], 'invalid index checksum'
	signature, version, num_entries = struct.unpack('!4sLL', data[:12])
	assert signature == b'DIRC', \
			'invalid index signature {}'.format(signature)
	assert version == 2, 'unknown index version {}'.format(version)
	entry_data = data[12:-20]
	entries = []
	i = 0
	while i + 62 < len(entry_data):
		fields_end = i + 62
		fields = struct.unpack('!LLLLLLLLLL20sH', entry_data[i:fields_end])
		path_end = entry_data.index(b'\x00', fields_end)
		path = entry_data[fields_end:path_end]
		entry = IndexEntry(*(fields + (path.decode(),)))
		entries.append(entry)
		entry_len = ((62 + len(path) + 8) // 8) * 8
		i += entry_len
	assert len(entries) == num_entries
	return entries

if __name__=="__main__":
	test_uid = "t_5669280306"
	test_vc = VersionControl(test_uid)
	test_dir = os.path.join('data', test_uid)
	file_path = os.path.join(test_dir, 'workspace/profile/test2.txt')
	sha1 = read_file(test_vc.master_profile).decode().strip()
	history_commit = test_vc.commit_history(sha1)
	entries_x = test_vc.read_commit(sha1)
	obj_type, data1 = test_vc._read_object(entries_x[0].sha1.hex())
	print(data1.decode())