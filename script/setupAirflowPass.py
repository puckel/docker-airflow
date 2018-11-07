import airflow
import argparse
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser

if __name__ == '__main__':
  ap = argparse.ArgumentParser()
  ap.add_argument("-u", "--username", default='airflow', help="Airflow login name")
  ap.add_argument("-e", "--email",    help="Airflow email",      required=True)
  ap.add_argument("-p", "--password", help="Airflow password",   required=True)
  username = vars(ap.parse_args())['username']
  email    = vars(ap.parse_args())['email']
  password = vars(ap.parse_args())['password']

  user = PasswordUser(models.User())
  user.username = username
  user.email = email
  user.password = password
  session = settings.Session()
  session.add(user)
  session.commit()
  session.close()
