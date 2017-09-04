import os
import boto3
import shutil
from multiprocessing import Process
import pywren

import numpy as np
import base64

ROOT_DIR = os.getcwd()

CONFIG_PATH = os.path.join(ROOT_DIR, ".pywren_config")
AWS_CREDS = os.path.join(ROOT_DIR, ".aws/credentials")

CREDS_TO_TRY = os.path.abspath("risecamp_pywren_creds")

ROOT_USER = 0
NUM_SUBUSERS = 20

def test_function(b):
  return "hello world"

def verify_user(root, subuser):
  base = "user_%02d.subuser.user_%02d.deploy" % (root, subuser)

  if os.path.exists(AWS_CREDS):
    os.unlink(AWS_CREDS)

  if os.path.exists(CONFIG_PATH):
    os.unlink(CONFIG_PATH)

  cred_file = base + ".creds"
  os.symlink(os.path.join(CREDS_TO_TRY, cred_file), AWS_CREDS)

  pywren_config = base + ".pywren_config.yaml"
  os.symlink(os.path.join(CREDS_TO_TRY, pywren_config), CONFIG_PATH)

  print(boto3.resource('iam').CurrentUser().arn)

  pwex = pywren.default_executor()
  futures = pwex.map(test_function, [0,1,2])

  results = [f.result(throw_except=False) for f in futures]
  if results != ["hello world", "hello world", "hello world"]:
    raise Exception("Problem executing lambda with account {} subuser {}".format(root, subuser) )
  print("success")

def verify_all():
  aws_dir = os.path.join(ROOT_DIR, ".aws")
  if os.path.exists(aws_dir):
    a = input("remove existing .aws creds?[yn]: ")
    if a[0] != 'y':
      exit()
    shutil.rmtree(aws_dir)
  os.makedirs(aws_dir)

  with open(os.path.join(aws_dir, "config"), 'w') as f:
    f.write("[default]\nregion = us-west-2")

  for user in range(NUM_SUBUSERS):
    # We need to fork a new process so boto3 loads the new credentials.
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = AWS_CREDS
    p = Process(target=verify_user, args=(ROOT_USER, user))
    p.start()
    p.join()

  shutil.rmtree(aws_dir)
  os.unlink(CONFIG_PATH)
  print()
  print("All done")

def base64_encode(config_string):
  return "PYWREN_CONFIG=" + base64.b64encode(config_string)

def create_env_file(root, subuser, src, target):
  base = "user_%02d.subuser.user_%02d.deploy" % (root, subuser)
  
  pywren_config = base + ".pywren_config.yaml"
  cred_file = base + ".creds"
  with open(os.path.join(target, base), "wb") as f:
    with open(os.path.join(src, cred_file), 'rb') as g:
      f.write(g.read())
    f.write("\n")
    with open(os.path.join(src, pywren_config), 'rb') as h:
      f.write(base64_encode(h.read()))

def generate_creds(src, target_dir):
  if os.path.exists(target_dir):
    a = input("remove existing directory? [yn]: ")
    if a[0] != 'y':
      exit()
    shutil.rmtree(target_dir)
  os.makedirs(target_dir)
  for user in range(NUM_SUBUSERS):
    create_env_file(ROOT_USER, user, src, target_dir)

if __name__ == '__main__':
  generate_creds(CREDS_TO_TRY, "creds_for_qifan")

