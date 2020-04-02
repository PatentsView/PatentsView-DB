import logging
from lib.configuration import get_section

logger = logging.getLogger("LIB: Notifications")


def send_slack_notification(message, config, section="DB Update", level="info"):
    """
    Send slack notification with given message formatted based on error level
    :param config: Config containing slack credentials
    :param message: Message to be posted
    :param section: Message Type
    :param level: Message level (error, warning, success or info)
    :return: API Response from Slack
    """
    from slackclient import SlackClient
    # Init
    color_map = {"info": "#6699cc", "success": "#aad922", "warning": "#FFFF00", "error": "#d62828"}

    # Connect to slack
    slack_token = config["SLACK"]["API_TOKEN"]
    slack_client = SlackClient(slack_token)
    slack_channel = config["SLACK"]["CHANNEL"]

    # Post the message
    return slack_client.api_call(
        "chat.postMessage",
        channel=slack_channel,
        text=section,
        attachments=[
            {
                "color": color_map[level],
                "text": message
            }
        ]
    )