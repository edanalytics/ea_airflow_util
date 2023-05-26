import textwrap

from airflow.providers.slack.notifications.slack import SlackNotifier


class SlackAlertFailure(SlackNotifier):
    """
    Extend the SlackNotifier with a default task failure message.
    """
    pass


class SlackAlertSuccess(SlackNotifier):
    """
    Extend the SlackNotifier with a default task success message.
    """
    pass


class SlackAlertSlaMiss(SlackNotifier):
    """
    Extend the SlackNotifier with a default SLA miss message.
    """
    pass

