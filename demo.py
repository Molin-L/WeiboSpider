import os

repo = "data/TEST"
paths = set()
for root, dirs, files in os.walk(os.path.join(repo, 'workspace')):
	dirs[:] = [d for d in dirs if d != '.git']
	for file in files:
		path = os.path.join(root, file)
		path = path.replace('\\', '/')
		if path.startswith('./'):
			path = path[2:]
		paths.add(path)

print(paths)