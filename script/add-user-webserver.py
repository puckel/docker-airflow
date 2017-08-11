"""
Adds a user to allow access to the airflow webserver.
See: https://airflow.incubator.apache.org/security.html#web-authentication
"""
import argparse
from airflow import models
from airflow import settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

parser = argparse.ArgumentParser(description='Add user to webserver web auth')
parser.add_argument('-u', '--username')
parser.add_argument('-e', '--email')
parser.add_argument('-p', '--password', required=True)

args = parser.parse_args()

user = PasswordUser(models.User())
if 'username' in args:
    user.username = args.username

if 'email' in args:
    user.email = args.email

if not user.username and not user.email:
    raise(KeyError('Neither username nor email specified for user login'))

user.password = args.password

session = settings.Session()
session.add(user)
session.commit()
session.close()
