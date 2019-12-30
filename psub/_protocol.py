"""Implementation of internal client-server protocol"""
from typing import NamedTuple

CMD_PUB = "PUB"
CMD_SUB = "SUB"
CMD_UNSUB = "USUB"


def build_message(command: str, data: str) -> bytes:
    return f"{command}::{data}".encode("utf8")


class ParsedMessage(NamedTuple):
    command: str
    topic: str
    data: str = None


def parse_command(message: bytes) -> ParsedMessage:
    command, data = [part.decode("utf8") for part in message.split(b"::", 1)]
    if command == CMD_SUB:
        topic, data = data.split(",", 1)
        return ParsedMessage(CMD_SUB, topic, data)
    return ParsedMessage(command, data)


def pub(topic, data):
    return build_message(CMD_PUB, f"{topic},{data}")


def sub(topic):
    return build_message(CMD_SUB, topic)


def unsub(topic):
    return build_message(CMD_UNSUB, topic)
