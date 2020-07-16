# astronomer-data
Dags for astronomer data org

# Prequisites:
- AWS account
- AWS cli installed
- Secrets manager access
- Astronomer account
- pyenv
- docker
- jq

# Install
1. Clone the repo
2. Create a new virtual environment
    - `pyenv virtualenv 3.6.4 astronomer`
3. Install python requirements
    - `pip install -r requirements.txt`
4. Install astronomercli
    - `brew install astronomer/tap/astro`
5. Login to astronomer
    - `astro auth login gcp0001.us-east4.astronomer.io`
6. Skip entering the username for oauth
7. Click on the link to get the oauth token
8. Copy the oauth token to the token field to login
9. Start the local environment
    - `make run-local`

# Troubleshooting
1. Pyenv believes aws to not be installed correctly
    - Install the aws cli via ui
    - `ln -s /usr/local/aws-cli/aws ~/.pyenv/shims/aws`
2.