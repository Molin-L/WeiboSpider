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

class ObjectType(enum.Enum):
	"""Object type enum. There are other types too, but we don't need them.
	See "enum object_type" in git's source (git/cache.h).
	"""
	commit = 1
	tree = 2
	blob = 3

class GitObject (object):

	repo = None

	def __init__(self, repo, data=None):
		self.repo=repo

		if data != None:
			self.deserialize(data)

	def serialize(self):
		raise Exception("Unimplemented!")

	def deserialize(self, data):
		raise Exception("Unimplemented!")
class GitBlob(GitObject):
	fmt=b'blob'

	def serialize(self):
		return self.blobdata

	def deserialize(self, data):
		self.blobdata = data

class GitCommit(GitObject):
	fmt=b'commit'

	def serialize(self):
		pass

	def deserialize(self, data):
		self.data = data
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

	def _add_file(self, mode="profile"):
		"""
		Write ProfileIndex.
		"""
		paths = set()
		if mode=='profile':
			workspace = self.profile_dir
		else:
			workspace = self.posts_dir
		for root, dirs, files in os.walk(workspace):
			dirs[:] = [d for d in dirs if d != '.git']
			for file in files:
				path = os.path.join(root, file)
				path = path.replace('\\', '/')
				if path.startswith('./'):
					path = path[2:]
				paths.add(path)
		entries = generate_entries(paths, self.repo)
		if mode=='profile':
			write_index(entries, self.profile_index)
		else:
			write_index(entries, self.posts_index)
	
	def update_profile(self):
		changed, new, _ = self._get_status('profile')

	def commit(self, mode='profile'):

		changed, newfile, delete = self._get_status(mode)
		if (len(changed)+len(newfile)+len(delete))<1:
			logger.info("Nothing to commit.")
			return
		
		self._add_file(mode)
		self._create_commit(mode)
		print(self.diff(mode))

	def read_commit(self, commit_sha1):
		obj_type, commit = self.read_object(commit_sha1)
		assert obj_type == 'commit'
		lines = commit.decode().splitlines()
		index_data = (l[6:] for l in lines if l.startswith('index '))
		for index in index_data:
			print(index)
			_, index_file = self.read_object(index)
			entries = read_index(data=index_file)
			print(entries)
			for entry in entries:
				print(entry.path)
			

	def commit_history(self, commit_sha1):
		obj_type, commit = self.read_object(commit_sha1)
		assert obj_type == 'commit'
		lines = commit.decode().splitlines()
		parents = (l[7:47] for l in lines if l.startswith('parent '))

		commit_history = []
		commit_history.append(commit_sha1)
		for parent in parents:
			commit_history+=self.read_commit(parent)
		return commit_history
	
	def read_object(self, sha1_prefix):
		"""Read object with given SHA-1 prefix and return tuple of
		(object_type, data_bytes), or raise ValueError if not found.
		"""
		path = self.find_object(sha1_prefix)
		full_data = zlib.decompress(read_file(path))
		nul_index = full_data.index(b'\x00')
		header = full_data[:nul_index]
		obj_type, size_str = header.decode().split()
		size = int(size_str)
		data = full_data[nul_index + 1:]
		assert size == len(data), 'expected size {}, got {} bytes'.format(
				size, len(data))
		return (obj_type, data)

	def find_object(self, sha1_prefix):
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
	
	def status(self, mode):
		changed, new, deleted = self._get_status(mode)
		if changed:
			print('changed files:')
			for path in changed:
				print('   ', path)
		if new:
			print('new files:')
			for path in new:
				print('   ', path)
		if deleted:
			print('deleted files:')
			for path in deleted:
				print('   ', path)	
	def diff(self, mode):
		"""Show diff of files changed (between index and working copy)."""
		if mode == "posts":
			index_dir = self.posts_index
		else:
			index_dir = self.profile_index
		changed, _, _ = self._get_status(mode)
		entries_by_path = {e.path: e for e in read_index(index_dir)}
		diff_data = dict()
		for i, path in enumerate(changed):
			sha1 = entries_by_path[path].sha1.hex()
			obj_type, data = self.read_object(sha1)
			assert obj_type == 'blob'
			index_lines = data.decode().splitlines()
			working_lines = read_file(path).decode().splitlines()
			diff_lines = difflib.unified_diff(
					index_lines, working_lines,
					'{} (index)'.format(path),
					'{} (working copy)'.format(path),
					lineterm='')
			for line in diff_lines:
				print(line)
			if i < len(changed) - 1:
				print('-' * 70)
			diff_data['']
	
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

		index_sha1 = hash_object(self.repo, index_data, 'index')
		lines.append('index {}'.format(index_sha1))
		data = '\n'.join(lines).encode()

		sha1 = hash_object(self.repo, data, 'commit')

		if mode=='posts':
			master_path = self.master_posts
		else:
			master_path = self.master_profile
		write_file(master_path, (sha1+'\n').encode())
		logger.info("Commit to {} master: {}".format(mode, sha1))
		return sha1

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
				if hash_object(self.repo, read_file(p), 'blob', write=False) !=
					entries_by_path[p].sha1.hex()}
		new = paths - entry_paths
		deleted = entry_paths - paths
		return (sorted(changed), sorted(new), sorted(deleted))


def object_hash(fd, fmt, repo=None):
	data = fd.read()

	# Choose constructor depending on
	# object type found in header.
	if fmt==b'blob': obj=GitBlob(repo, data)
	else:
		raise Exception("Unknown type %s!" % fmt)
	'''
	if   fmt==b'commit' : obj=GitCommit(repo, data)
	elif fmt==b'tree'   : obj=GitTree(repo, data)
	elif fmt==b'tag'    : obj=GitTag(repo, data)
	elif fmt==b'blob'   : obj=GitBlob(repo, data)
	else:
		raise Exception("Unknown type %s!" % fmt)
	'''

	return object_write(obj)

def object_write(obj):
	# Serialize object data
	data = obj.serialize()
	# Add header
	result = obj.fmt + b' ' + str(len(data)).encode() + b'\x00' + data
	# Compute hash
	sha1 = hashlib.sha1(result).hexdigest()
	path=os.path.join(obj.repo, 'versions', 'objects', sha1[:2], sha1[2:])
	if not os.path.exists(path):
			os.makedirs(os.path.dirname(path), exist_ok=True)
			with open(path, 'wb') as f:
				# Compress and write
				f.write(zlib.compress(result))

	return sha1

def hash_object(repo_dir, data, obj_type, write=True):
	header = '{} {}'.format(obj_type, len(data)).encode()
	full_data = header + b'\x00' + data
	sha1 = hashlib.sha1(full_data).hexdigest()
	if write:
		path = os.path.join(repo_dir, 'versions', 'objects', sha1[:2], sha1[2:])
		if not os.path.exists(path):
			os.makedirs(os.path.dirname(path), exist_ok=True)
			write_file(path, zlib.compress(full_data))
	return sha1

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

def write_index(entries, outpath):
	"""Write list of IndexEntry objects to git index file."""
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
	
	write_file(outpath, all_data + digest)

def read_file(path):
	"""Read contents of file at given path as bytes."""
	with open(path, 'rb') as f:
		return f.read()


def write_file(path, data):
	"""Write data bytes to file at given path."""
	with open(path, 'wb') as f:
		f.write(data)

def generate_entries(paths, repo):
	entries = []
	for path in paths:
		with open(path, 'rb') as fd:
			sha1 = object_hash(fd, b'blob', repo)
			print(sha1)
		st = os.stat(path)
		flags = len(path.encode())
		assert flags < (1 << 12)
		entry = IndexEntry(
				int(st.st_ctime), 0, int(st.st_mtime), 0, st.st_dev,
				st.st_ino, st.st_mode, st.st_uid, st.st_gid, st.st_size,
				bytes.fromhex(sha1), flags, path)
		entries.append(entry)
	return entries

if __name__ == "__main__":
	test_uid = "t_5669280306"
	test_vc = VersionControl(test_uid)
	test_vc._add_file(mode="profile")
	#test_vc.commit("profile")
	#sha1 = read_file(test_vc.master_posts).decode().strip()
	#test_vc.read_commit(sha1)
	#print(read_index_data(read_file(test_vc.profile_index)))
	test_vc.status("profile")
	