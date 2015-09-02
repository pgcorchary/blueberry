#!/usr/bin/env python

# from the polaris project
# audit users between cassandra file manifest and actual swift container

# if you have problem piping this script output breaking on UnicodeEncodeError, set this in your bash shell
# export PYTHONIOENCODING=utf-8


from cassandra.cluster import Cluster
import sys,os, yaml
import simplejson
import keystoneclient.v2_0 as keystoneclient
import swiftclient

# validate arguments
total = len(sys.argv)
if total == 2:
  userId = sys.argv[1]
  if len(userId) != 64:
    print "This doesn't look like a valid polaris userId"
    sys.exit()
else:
  print "Expected one argument, which must be the userId"
  sys.exit()

# try to get credentials from ~/.bangrc
home = os.getenv("HOME")
try:
  with open('%s/.bangrc' % home, 'r') as f:
    results = yaml.load(f)
  f.close()
except IOError:
  print 'ERROR: cannot read credentials file %s/.bangrc' % home
  sys.exit(1)

# gather credentials
os = results['deployer_credentials']['hpcloud_v13']

# open swift connection
opts = dict(tenant_id=os['tenant_id'], region_name=os['region_name'])
try:
  client = swiftclient.Connection(auth_version='2.0', authurl=os['auth_url'], tenant_name=os['tenant_name'], user=os['username'], key=os['password'], os_options=opts)
  print "SWIFT CONNECTION OK - user %s" % client.user
except:
  print "can't get SWIFT connection"
  sys.exit()

# open cassandra session
cass_cluster = ['10.0.0.173','10.0.0.155','10.0.0.162','10.0.0.148','10.0.0.161','10.0.0.163','10.0.0.159','10.0.0.157','10.0.0.158','10.0.0.145']
keyspace = 'polaris'
cluster = Cluster(cass_cluster, cql_version='3.0.4')
try:
  session = cluster.connect(keyspace)
  print "CASSANDRA CONNECTION OK  - keyspace %s" % keyspace
except:
  print "can't get cassandra connection"
  sys.exit()

def object_validate (container, obj):
  try:
    headers = client.head_object(container, obj)
    cl = headers.get('content-length', 'None')
    if cl is not None:
      code = 0
    else:
      code = 1
  except swiftclient.ClientException as err:
    code = str(err.http_status)
    cl = 0
  return (code, cl)

#all files for the user, one in each row
rows = session.execute("SELECT * FROM swift_directory_cf WHERE user_id=%s", (userId,))
n = len(rows)
if n > 0:
  print "FILES for user %s = %s" % (userId, len(rows))
else:
  print "No files found for user"
  sys.exit()

#iterate over all user files
count = 0
for row in rows:
  count += 1
  # get the json file manifest
  json = simplejson.loads(row.json)
  if 'chunks' in json.keys():
    chunks = json['chunks']
    chunk_num = len(json['chunks'])
    valid_chunks = 0
    # check each chunk listed in the json manifest
    for chunk_name in sorted(chunks):
      obj_container =  chunks[chunk_name]['container']
      obj_seg = chunks[chunk_name]['swift_path']
      chunk_code, chunk_content_len = object_validate(obj_container,obj_seg)
      if chunk_code == 0:
        valid_chunks +=1
    if chunk_num == valid_chunks:
      file_state_flag = "  OK"
    else:
      file_state_flag = "FAIL"
  else:  # there are no chunks in the manifest
    chunk_num = 0
    if json['size'] == 0:  # the file has zero length and was never uploaded
      file_state_flag = "FAIL"
      valid_chunks = 0
    else: # the file is contained totally in one file object (no 'chunks')
      obj_container =  json['container']
      obj_seg = json['swift_path']
      chunk_code, chunk_content_len = object_validate(obj_container,obj_seg)
      if chunk_code == 0:
        valid_chunks = 1
        file_state_flag = "  OK"
  msg = ""
  if (json['size'] > 1000000 and chunk_num == 0):
    file_state_flag = "UKNW"
    chunk_num = 1
    msg = "*expected multi-chunks for size=" + str(chunk_content_len) + " code=" + str(chunk_code)
  file_date = json['last-modified']
  print "%s #%3s <<%s>>, %sb, %s chunks, valid-chunks=%s date=%s %s" % (file_state_flag, count, row.path, json['size'], chunk_num, valid_chunks, file_date, msg)
# end for loop

print "DONE %s files checked for user" % count

