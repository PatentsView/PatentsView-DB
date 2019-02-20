from slackclient import SlackClient
import configparser
import os
project_home=os.environ['PACKAGE_HOME']
config = configparser.ConfigParser()
config.read(project_home+'/Development/config.ini')
slack_token=config["SLACK"]["API_TOKEN"]
slack_client=SlackClient(slack_token)
slack_channel=config["SLACK"]["CHANNEL"]