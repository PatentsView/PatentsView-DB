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
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError

    # Init
    color_map = {"info": "#6699cc", "success": "#aad922", "warning": "#FFFF00", "error": "#d62828"}

    # Connect to slack
    slack_token = config["SLACK"]["API_TOKEN"]
    slack_client = WebClient(slack_token)
    slack_channel = config["SLACK"]["CHANNEL"]
    client = WebClient(token=token)
    try:
        response=slack_client.chat_postMessage(
        channel=slack_channel,
        text=section,
        attachments=[
            {
                "color": color_map[level],
                "text": message
            }
        ]
    )
    except SlackApiError as e:
        print(e)
    return response.status_code