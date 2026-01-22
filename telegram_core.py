"""
Telegram Core Module - Shared functionality for MCP and HTTP API.

This module contains the core Telegram client and all business logic functions
that can be used by both the MCP server and the HTTP API.
"""

import os
import json
import logging
import re
import asyncio
import random
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Dict, Optional, Union, Any
from functools import wraps

from dotenv import load_dotenv
from pythonjsonlogger import jsonlogger
from telethon import TelegramClient, functions, utils
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError as TelethonFloodWaitError
from telethon.tl.types import (
    User,
    Chat,
    Channel,
    ChatAdminRights,
    ChatBannedRights,
    ChannelParticipantsKicked,
    ChannelParticipantsAdmins,
    InputChatPhotoEmpty,
)

# Load environment variables
load_dotenv()

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "telegram_session")
SESSION_STRING = os.getenv("TELEGRAM_SESSION_STRING")


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class ErrorCategory(str, Enum):
    CHAT = "CHAT"
    MSG = "MSG"
    CONTACT = "CONTACT"
    GROUP = "GROUP"
    MEDIA = "MEDIA"
    PROFILE = "PROFILE"
    AUTH = "AUTH"
    ADMIN = "ADMIN"


# Setup logging
logger = logging.getLogger("telegram_core")
logger.setLevel(logging.ERROR)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# Try to set up file logging
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(script_dir, "telegram_core.log")

try:
    file_handler = logging.FileHandler(log_file_path, mode="a")
    file_handler.setLevel(logging.ERROR)
    json_formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    file_handler.setFormatter(json_formatter)
    logger.addHandler(file_handler)
except Exception:
    pass


def json_serializer(obj):
    """Helper function to convert non-serializable objects for JSON serialization."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def log_and_format_error(
    function_name: str,
    error: Exception,
    prefix: Optional[Union[ErrorCategory, str]] = None,
    user_message: str = None,
    **kwargs,
) -> str:
    """Centralized error handling function."""
    if isinstance(prefix, str) and prefix == "VALIDATION-001":
        error_code = prefix
    else:
        if prefix is None:
            for category in ErrorCategory:
                if category.name.lower() in function_name.lower():
                    prefix = category
                    break
        prefix_str = prefix.value if isinstance(prefix, ErrorCategory) else (prefix or "GEN")
        error_code = f"{prefix_str}-ERR-{abs(hash(function_name)) % 1000:03d}"

    context = ", ".join(f"{k}={v}" for k, v in kwargs.items())
    logger.error(f"Error in {function_name} ({context}) - Code: {error_code}", exc_info=True)

    if user_message:
        return user_message
    return f"An error occurred (code: {error_code}). Check logs for details."


def validate_id_value(value, param_name: str):
    """Validate a single ID value. Returns (validated_value, error_message)."""
    if isinstance(value, int):
        if not (-(2**63) <= value <= 2**63 - 1):
            return None, f"Invalid {param_name}: {value}. ID is out of range."
        return value, None

    if isinstance(value, str):
        try:
            int_value = int(value)
            if not (-(2**63) <= int_value <= 2**63 - 1):
                return None, f"Invalid {param_name}: {value}. ID is out of range."
            return int_value, None
        except ValueError:
            if re.match(r"^@?[a-zA-Z0-9_]{5,}$", value):
                return value, None
            else:
                return None, f"Invalid {param_name}: '{value}'. Must be integer ID or username."

    return None, f"Invalid {param_name}: {value}. Type must be int or str."


def validate_ids(param_name: str, param_value):
    """Validate ID parameter(s). Returns (validated_value, error_message)."""
    if param_value is None:
        return None, None

    if isinstance(param_value, list):
        validated_list = []
        for item in param_value:
            validated_item, error_msg = validate_id_value(item, param_name)
            if error_msg:
                return None, error_msg
            validated_list.append(validated_item)
        return validated_list, None
    else:
        return validate_id_value(param_value, param_name)


def format_entity(entity) -> Dict[str, Any]:
    """Helper function to format entity information consistently."""
    result = {"id": entity.id}

    if hasattr(entity, "title"):
        result["name"] = entity.title
        result["type"] = "group" if isinstance(entity, Chat) else "channel"
    elif hasattr(entity, "first_name"):
        name_parts = []
        if entity.first_name:
            name_parts.append(entity.first_name)
        if hasattr(entity, "last_name") and entity.last_name:
            name_parts.append(entity.last_name)
        result["name"] = " ".join(name_parts)
        result["type"] = "user"
        if hasattr(entity, "username") and entity.username:
            result["username"] = entity.username
        if hasattr(entity, "phone") and entity.phone:
            result["phone"] = entity.phone

    return result


def format_message(message) -> Dict[str, Any]:
    """Helper function to format message information consistently."""
    result = {
        "id": message.id,
        "date": message.date.isoformat(),
        "text": message.message or "",
    }

    if message.from_id:
        result["from_id"] = utils.get_peer_id(message.from_id)

    if message.media:
        result["has_media"] = True
        result["media_type"] = type(message.media).__name__

    return result


def get_sender_name(message) -> str:
    """Helper function to get sender name from a message."""
    if not message.sender:
        return "Unknown"

    if hasattr(message.sender, "title") and message.sender.title:
        return message.sender.title
    elif hasattr(message.sender, "first_name"):
        first_name = getattr(message.sender, "first_name", "") or ""
        last_name = getattr(message.sender, "last_name", "") or ""
        full_name = f"{first_name} {last_name}".strip()
        return full_name if full_name else "Unknown"
    else:
        return "Unknown"


def get_engagement_info(message) -> str:
    """Helper function to get engagement metrics from a message."""
    engagement_parts = []
    views = getattr(message, "views", None)
    if views is not None:
        engagement_parts.append(f"views:{views}")
    forwards = getattr(message, "forwards", None)
    if forwards is not None:
        engagement_parts.append(f"forwards:{forwards}")
    reactions = getattr(message, "reactions", None)
    if reactions is not None:
        results = getattr(reactions, "results", None)
        total_reactions = sum(getattr(r, "count", 0) or 0 for r in results) if results else 0
        engagement_parts.append(f"reactions:{total_reactions}")
    return f" | {', '.join(engagement_parts)}" if engagement_parts else ""


class TelegramCore:
    """Core Telegram functionality that can be used by both MCP and HTTP API."""

    def __init__(self):
        self.client: Optional[TelegramClient] = None
        self._started = False
        
        # Rate limiting tracking
        self._last_request_time: Optional[float] = None
        self._last_message_time_per_chat: Dict[Union[int, str], float] = {}
        self._last_edit_time: Optional[float] = None
        self._edit_count_last_hour: int = 0
        self._edit_count_reset_time: Optional[float] = None
        self.min_request_delay: float = 0.2  # 5 req/s

    async def start(self):
        """Initialize and start the Telegram client."""
        if self._started:
            return

        if SESSION_STRING:
            self.client = TelegramClient(
                StringSession(SESSION_STRING), TELEGRAM_API_ID, TELEGRAM_API_HASH
            )
        else:
            self.client = TelegramClient(
                TELEGRAM_SESSION_NAME, TELEGRAM_API_ID, TELEGRAM_API_HASH
            )

        await self.client.start()
        self._started = True

    async def stop(self):
        """Stop the Telegram client."""
        if self.client and self._started:
            await self.client.disconnect()
            self._started = False

    # ==================== Rate Limiting ====================
    
    async def _wait_for_rate_limit(self):
        """Ожидание перед следующим запросом для соблюдения rate limits."""
        import time
        current_time = time.time()
        
        if self._last_request_time is not None:
            elapsed = current_time - self._last_request_time
            if elapsed < self.min_request_delay:
                sleep_time = self.min_request_delay - elapsed
                # Добавляем небольшой jitter для избежания синхронизации
                sleep_time += random.uniform(0, 0.05)
                await asyncio.sleep(sleep_time)
        
        self._last_request_time = time.time()

    async def _check_edit_rate_limit(self):
        """Проверка лимита редактирования (5 edits/s, 120 edits/hour)."""
        import time
        current_time = time.time()
        
        # Сброс счетчика каждый час
        if self._edit_count_reset_time is None or current_time >= self._edit_count_reset_time:
            self._edit_count_last_hour = 0
            self._edit_count_reset_time = current_time + 3600
        
        # Проверка лимита 120 редактирований в час
        if self._edit_count_last_hour >= 120:
            wait_time = self._edit_count_reset_time - current_time
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                # Пересчитываем после ожидания
                current_time = time.time()
                if current_time >= self._edit_count_reset_time:
                    self._edit_count_last_hour = 0
                    self._edit_count_reset_time = current_time + 3600
        
        # Проверка лимита 5 редактирований в секунду
        if self._last_edit_time is not None:
            elapsed = current_time - self._last_edit_time
            if elapsed < 0.2:  # 5 edits/s = 1 edit per 0.2s
                sleep_time = 0.2 - elapsed + random.uniform(0, 0.02)
                await asyncio.sleep(sleep_time)
        
        self._last_edit_time = time.time()
        self._edit_count_last_hour += 1

    async def _check_message_rate_limit(self, chat_id: Union[int, str]):
        """Проверка лимита отправки сообщений (1 msg/s в один чат)."""
        import time
        current_time = time.time()
        
        if chat_id in self._last_message_time_per_chat:
            elapsed = current_time - self._last_message_time_per_chat[chat_id]
            if elapsed < 1.0:  # Минимум 1 секунда между сообщениями в один чат
                sleep_time = 1.0 - elapsed + random.uniform(0, 0.1)
                await asyncio.sleep(sleep_time)
        
        self._last_message_time_per_chat[chat_id] = time.time()

    # ==================== Chat Operations ====================

    async def get_chats(self, page: int = 1, page_size: int = 20) -> str:
        """Get a paginated list of chats."""
        try:
            dialogs = await self.client.get_dialogs()
            start = (page - 1) * page_size
            end = start + page_size
            if start >= len(dialogs):
                return "Page out of range."
            chats = dialogs[start:end]
            lines = []
            for dialog in chats:
                entity = dialog.entity
                chat_id = entity.id
                title = getattr(entity, "title", None) or getattr(entity, "first_name", "Unknown")
                lines.append(f"Chat ID: {chat_id}, Title: {title}")
            return "\n".join(lines)
        except Exception as e:
            return log_and_format_error("get_chats", e)

    async def list_chats(
        self,
        limit: int = 50,
        chat_type: Optional[str] = None,
        archived: bool = False,
        unread_only: bool = False,
    ) -> str:
        """Get a filtered list of chats with metadata."""
        try:
            dialogs = await self.client.get_dialogs(limit=limit, archived=archived)

            results = []
            for dialog in dialogs:
                entity = dialog.entity

                if chat_type:
                    if chat_type == "user" and not isinstance(entity, User):
                        continue
                    elif chat_type == "group" and not isinstance(entity, Chat):
                        continue
                    elif chat_type == "channel" and not isinstance(entity, Channel):
                        continue

                if unread_only and dialog.unread_count == 0:
                    continue

                chat_info = format_entity(entity)
                chat_info["unread_count"] = dialog.unread_count
                chat_info["is_pinned"] = dialog.pinned

                results.append(chat_info)

            return json.dumps(results, indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("list_chats", e, limit=limit, chat_type=chat_type)

    async def get_chat(self, chat_id: Union[int, str]) -> str:
        """Get detailed information about a specific chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            info = format_entity(entity)

            if isinstance(entity, (Chat, Channel)):
                full_chat = await self.client(functions.channels.GetFullChannelRequest(entity))
                if hasattr(full_chat, "full_chat"):
                    info["about"] = getattr(full_chat.full_chat, "about", None)
                    info["participants_count"] = getattr(
                        full_chat.full_chat, "participants_count", None
                    )

            return json.dumps(info, indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("get_chat", e, chat_id=chat_id)

    # ==================== Message Operations ====================

    async def get_messages(
        self, chat_id: Union[int, str], page: int = 1, page_size: int = 20
    ) -> str:
        """Get paginated messages from a specific chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            # Rate limiting protection for read operations
            await self._wait_for_rate_limit()
            try:
                entity = await self.client.get_entity(chat_id)
            except TelethonFloodWaitError as e:
                wait_time = min(float(getattr(e, 'seconds', 0)), 3600.0)
                if wait_time > 0:
                    await asyncio.sleep(wait_time + random.uniform(0, 1))
                    entity = await self.client.get_entity(chat_id)
                else:
                    raise
            
            await self._wait_for_rate_limit()
            offset = (page - 1) * page_size
            try:
                messages = await self.client.get_messages(entity, limit=page_size, add_offset=offset)
            except TelethonFloodWaitError as e:
                wait_time = min(float(getattr(e, 'seconds', 0)), 3600.0)
                if wait_time > 0:
                    await asyncio.sleep(wait_time + random.uniform(0, 1))
                    messages = await self.client.get_messages(entity, limit=page_size, add_offset=offset)
                else:
                    raise
            
            if not messages:
                return "No messages found for this page."
            lines = []
            for msg in messages:
                sender_name = get_sender_name(msg)
                reply_info = ""
                if msg.reply_to and msg.reply_to.reply_to_msg_id:
                    reply_info = f" | reply to {msg.reply_to.reply_to_msg_id}"
                engagement_info = get_engagement_info(msg)
                lines.append(
                    f"ID: {msg.id} | {sender_name} | Date: {msg.date}{reply_info}{engagement_info} | Message: {msg.message}"
                )
            return "\n".join(lines)
        except Exception as e:
            return log_and_format_error("get_messages", e, chat_id=chat_id, page=page)

    async def send_message(
        self,
        chat_id: Union[int, str],
        message: str,
        reply_to: Optional[int] = None,
        parse_mode: Optional[str] = None,
    ) -> str:
        """Send a message to a chat with rate limiting protection."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        # Проверка лимита сообщений для конкретного чата
        await self._check_message_rate_limit(chat_id)
        
        # Ожидание перед запросом для соблюдения общего rate limit
        await self._wait_for_rate_limit()

        try:
            entity = await self.client.get_entity(chat_id)
            result = await self.client.send_message(
                entity, message, reply_to=reply_to, parse_mode=parse_mode
            )
            return f"Message sent successfully. Message ID: {result.id}"
        except TelethonFloodWaitError as e:
            # Обработка FLOOD_WAIT от Telethon
            wait_time = min(float(e.seconds), 3600.0)  # Максимум 1 час
            await asyncio.sleep(wait_time + random.uniform(0, 1))
            # Повторная попытка после ожидания
            try:
                entity = await self.client.get_entity(chat_id)
                result = await self.client.send_message(
                    entity, message, reply_to=reply_to, parse_mode=parse_mode
                )
                return f"Message sent successfully. Message ID: {result.id}"
            except Exception as retry_e:
                return log_and_format_error("send_message", retry_e, chat_id=chat_id)
        except Exception as e:
            return log_and_format_error("send_message", e, chat_id=chat_id)

    async def edit_message(
        self, chat_id: Union[int, str], message_id: int, new_text: str
    ) -> str:
        """Edit a message with rate limiting protection."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        # Проверка лимита редактирования
        await self._check_edit_rate_limit()
        
        # Ожидание перед запросом для соблюдения общего rate limit
        await self._wait_for_rate_limit()

        try:
            entity = await self.client.get_entity(chat_id)
            await self.client.edit_message(entity, message_id, new_text)
            return f"Message {message_id} edited successfully."
        except TelethonFloodWaitError as e:
            # Обработка FLOOD_WAIT от Telethon
            wait_time = min(float(e.seconds), 3600.0)  # Максимум 1 час
            await asyncio.sleep(wait_time + random.uniform(0, 1))
            # Повторная попытка после ожидания
            try:
                entity = await self.client.get_entity(chat_id)
                await self.client.edit_message(entity, message_id, new_text)
                return f"Message {message_id} edited successfully."
            except Exception as retry_e:
                return log_and_format_error("edit_message", retry_e, chat_id=chat_id, message_id=message_id)
        except Exception as e:
            return log_and_format_error("edit_message", e, chat_id=chat_id, message_id=message_id)

    async def delete_message(
        self, chat_id: Union[int, str], message_id: int, revoke: bool = True
    ) -> str:
        """Delete a message."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            await self.client.delete_messages(entity, message_id, revoke=revoke)
            return f"Message {message_id} deleted successfully."
        except Exception as e:
            return log_and_format_error("delete_message", e, chat_id=chat_id, message_id=message_id)

    async def forward_message(
        self, from_chat_id: Union[int, str], to_chat_id: Union[int, str], message_id: int
    ) -> str:
        """Forward a message from one chat to another with rate limiting protection."""
        from_chat_id, error = validate_ids("from_chat_id", from_chat_id)
        if error:
            return error
        to_chat_id, error = validate_ids("to_chat_id", to_chat_id)
        if error:
            return error

        # Проверка лимита сообщений для целевого чата
        await self._check_message_rate_limit(to_chat_id)
        
        # Ожидание перед запросом для соблюдения общего rate limit
        await self._wait_for_rate_limit()

        try:
            from_entity = await self.client.get_entity(from_chat_id)
            to_entity = await self.client.get_entity(to_chat_id)
            result = await self.client.forward_messages(to_entity, message_id, from_entity)
            return f"Message forwarded successfully. New message ID: {result[0].id}"
        except TelethonFloodWaitError as e:
            # Обработка FLOOD_WAIT от Telethon
            wait_time = min(float(e.seconds), 3600.0)  # Максимум 1 час
            await asyncio.sleep(wait_time + random.uniform(0, 1))
            # Повторная попытка после ожидания
            try:
                from_entity = await self.client.get_entity(from_chat_id)
                to_entity = await self.client.get_entity(to_chat_id)
                result = await self.client.forward_messages(to_entity, message_id, from_entity)
                return f"Message forwarded successfully. New message ID: {result[0].id}"
            except Exception as retry_e:
                return log_and_format_error(
                    "forward_message", retry_e, from_chat_id=from_chat_id, to_chat_id=to_chat_id
                )
        except Exception as e:
            return log_and_format_error(
                "forward_message", e, from_chat_id=from_chat_id, to_chat_id=to_chat_id
            )

    async def search_messages(
        self,
        chat_id: Union[int, str],
        query: str,
        limit: int = 20,
        from_user: Optional[Union[int, str]] = None,
    ) -> str:
        """Search for messages in a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            from_entity = None
            if from_user:
                from_user, error = validate_ids("from_user", from_user)
                if error:
                    return error
                from_entity = await self.client.get_entity(from_user)

            messages = await self.client.get_messages(
                entity, limit=limit, search=query, from_user=from_entity
            )

            if not messages:
                return "No messages found matching the search."

            results = []
            for msg in messages:
                results.append(format_message(msg))

            return json.dumps(results, indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("search_messages", e, chat_id=chat_id, query=query)

    # ==================== Contact Operations ====================

    async def list_contacts(self) -> str:
        """Get all contacts."""
        try:
            result = await self.client(functions.contacts.GetContactsRequest(hash=0))
            contacts = []
            for user in result.users:
                contacts.append(format_entity(user))
            return json.dumps(contacts, indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("list_contacts", e)

    async def search_contacts(self, query: str, limit: int = 10) -> str:
        """Search contacts by name or username."""
        try:
            result = await self.client(functions.contacts.SearchRequest(q=query, limit=limit))
            contacts = []
            for user in result.users:
                contacts.append(format_entity(user))
            return json.dumps(contacts, indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("search_contacts", e, query=query)

    async def add_contact(
        self,
        phone: str,
        first_name: str,
        last_name: Optional[str] = None,
    ) -> str:
        """Add a new contact."""
        try:
            result = await self.client(
                functions.contacts.ImportContactsRequest(
                    contacts=[
                        functions.InputPhoneContact(
                            client_id=0,
                            phone=phone,
                            first_name=first_name,
                            last_name=last_name or "",
                        )
                    ]
                )
            )
            if result.users:
                return f"Contact added successfully: {format_entity(result.users[0])}"
            return "Contact added but user not found on Telegram."
        except Exception as e:
            return log_and_format_error("add_contact", e, phone=phone)

    async def delete_contact(self, user_id: Union[int, str]) -> str:
        """Delete a contact."""
        user_id, error = validate_ids("user_id", user_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(user_id)
            await self.client(functions.contacts.DeleteContactsRequest(id=[entity]))
            return f"Contact {user_id} deleted successfully."
        except Exception as e:
            return log_and_format_error("delete_contact", e, user_id=user_id)

    # ==================== User & Profile Operations ====================

    async def get_me(self) -> str:
        """Get information about the current user."""
        try:
            me = await self.client.get_me()
            info = format_entity(me)
            return json.dumps(info, indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("get_me", e)

    async def get_user_status(self, user_id: Union[int, str]) -> str:
        """Get the online status of a user."""
        user_id, error = validate_ids("user_id", user_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(user_id)
            if not isinstance(entity, User):
                return "Entity is not a user."

            status = entity.status
            status_info = {
                "user_id": entity.id,
                "status_type": type(status).__name__ if status else "Unknown",
            }

            if hasattr(status, "was_online"):
                status_info["was_online"] = status.was_online.isoformat()

            return json.dumps(status_info, indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("get_user_status", e, user_id=user_id)

    # ==================== Group Operations ====================

    async def create_group(self, title: str, users: List[Union[int, str]]) -> str:
        """Create a new group chat."""
        users, error = validate_ids("users", users)
        if error:
            return error

        try:
            user_entities = []
            for user_id in users:
                entity = await self.client.get_entity(user_id)
                user_entities.append(entity)

            result = await self.client(
                functions.messages.CreateChatRequest(title=title, users=user_entities)
            )
            chat_id = result.chats[0].id
            return f"Group '{title}' created successfully. Chat ID: {chat_id}"
        except Exception as e:
            return log_and_format_error("create_group", e, title=title)

    async def invite_to_group(
        self, chat_id: Union[int, str], user_ids: List[Union[int, str]]
    ) -> str:
        """Invite users to a group or channel."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error
        user_ids, error = validate_ids("user_ids", user_ids)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            user_entities = []
            for user_id in user_ids:
                user_entity = await self.client.get_entity(user_id)
                user_entities.append(user_entity)

            if isinstance(entity, Channel):
                await self.client(
                    functions.channels.InviteToChannelRequest(channel=entity, users=user_entities)
                )
            else:
                for user in user_entities:
                    await self.client(
                        functions.messages.AddChatUserRequest(
                            chat_id=entity.id, user_id=user, fwd_limit=50
                        )
                    )

            return f"Users invited to chat {chat_id} successfully."
        except Exception as e:
            return log_and_format_error("invite_to_group", e, chat_id=chat_id)

    async def leave_chat(self, chat_id: Union[int, str]) -> str:
        """Leave a group or channel."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            if isinstance(entity, Channel):
                await self.client(functions.channels.LeaveChannelRequest(channel=entity))
            else:
                await self.client(functions.messages.DeleteChatUserRequest(
                    chat_id=entity.id, user_id="me"
                ))
            return f"Left chat {chat_id} successfully."
        except Exception as e:
            return log_and_format_error("leave_chat", e, chat_id=chat_id)

    async def get_participants(
        self, chat_id: Union[int, str], limit: int = 100, offset: int = 0
    ) -> str:
        """Get participants of a group or channel."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            participants = await self.client.get_participants(entity, limit=limit, offset=offset)

            results = []
            for user in participants:
                results.append(format_entity(user))

            return json.dumps(results, indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("get_participants", e, chat_id=chat_id)

    # ==================== Admin Operations ====================

    async def get_admins(self, chat_id: Union[int, str]) -> str:
        """Get administrators of a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            if isinstance(entity, Channel):
                participants = await self.client.get_participants(
                    entity, filter=ChannelParticipantsAdmins
                )
            else:
                full_chat = await self.client(functions.messages.GetFullChatRequest(entity.id))
                admin_ids = [
                    p.user_id for p in full_chat.full_chat.participants.participants
                    if hasattr(p, "admin_rights") or getattr(p, "is_admin", False)
                ]
                participants = [
                    user for user in full_chat.users if user.id in admin_ids
                ]

            results = []
            for user in participants:
                results.append(format_entity(user))

            return json.dumps(results, indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("get_admins", e, chat_id=chat_id)

    async def promote_admin(
        self,
        chat_id: Union[int, str],
        user_id: Union[int, str],
        title: Optional[str] = None,
    ) -> str:
        """Promote a user to admin."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error
        user_id, error = validate_ids("user_id", user_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            user = await self.client.get_entity(user_id)

            rights = ChatAdminRights(
                change_info=True,
                delete_messages=True,
                ban_users=True,
                invite_users=True,
                pin_messages=True,
                manage_call=True,
            )

            await self.client(
                functions.channels.EditAdminRequest(
                    channel=entity, user_id=user, admin_rights=rights, rank=title or ""
                )
            )

            return f"User {user_id} promoted to admin in chat {chat_id}."
        except Exception as e:
            return log_and_format_error("promote_admin", e, chat_id=chat_id, user_id=user_id)

    async def ban_user(
        self, chat_id: Union[int, str], user_id: Union[int, str], until_date: Optional[int] = None
    ) -> str:
        """Ban a user from a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error
        user_id, error = validate_ids("user_id", user_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            user = await self.client.get_entity(user_id)

            rights = ChatBannedRights(
                until_date=until_date,
                view_messages=True,
                send_messages=True,
                send_media=True,
                send_stickers=True,
                send_gifs=True,
            )

            await self.client(
                functions.channels.EditBannedRequest(channel=entity, participant=user, banned_rights=rights)
            )

            return f"User {user_id} banned from chat {chat_id}."
        except Exception as e:
            return log_and_format_error("ban_user", e, chat_id=chat_id, user_id=user_id)

    async def unban_user(self, chat_id: Union[int, str], user_id: Union[int, str]) -> str:
        """Unban a user from a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error
        user_id, error = validate_ids("user_id", user_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            user = await self.client.get_entity(user_id)

            rights = ChatBannedRights(until_date=None, view_messages=False)

            await self.client(
                functions.channels.EditBannedRequest(channel=entity, participant=user, banned_rights=rights)
            )

            return f"User {user_id} unbanned from chat {chat_id}."
        except Exception as e:
            return log_and_format_error("unban_user", e, chat_id=chat_id, user_id=user_id)

    # ==================== Channel Operations ====================

    async def get_invite_link(self, chat_id: Union[int, str]) -> str:
        """Get the invite link for a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)

            if isinstance(entity, Channel):
                result = await self.client(
                    functions.messages.ExportChatInviteRequest(peer=entity)
                )
                return f"Invite link: {result.link}"
            else:
                result = await self.client(
                    functions.messages.ExportChatInviteRequest(peer=entity)
                )
                return f"Invite link: {result.link}"
        except Exception as e:
            return log_and_format_error("get_invite_link", e, chat_id=chat_id)

    async def resolve_username(self, username: str) -> str:
        """Resolve a username to get entity information."""
        try:
            entity = await self.client.get_entity(username)
            return json.dumps(format_entity(entity), indent=2, default=json_serializer)
        except Exception as e:
            return log_and_format_error("resolve_username", e, username=username)

    # ==================== Notification Operations ====================

    async def mute_chat(self, chat_id: Union[int, str], mute_until: Optional[int] = None) -> str:
        """Mute notifications for a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            settings = functions.InputPeerNotifySettings(
                mute_until=mute_until or 2147483647  # Max int = forever
            )
            await self.client(
                functions.account.UpdateNotifySettingsRequest(
                    peer=functions.InputNotifyPeer(peer=entity), settings=settings
                )
            )
            return f"Chat {chat_id} muted successfully."
        except Exception as e:
            return log_and_format_error("mute_chat", e, chat_id=chat_id)

    async def unmute_chat(self, chat_id: Union[int, str]) -> str:
        """Unmute notifications for a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            settings = functions.InputPeerNotifySettings(mute_until=0)
            await self.client(
                functions.account.UpdateNotifySettingsRequest(
                    peer=functions.InputNotifyPeer(peer=entity), settings=settings
                )
            )
            return f"Chat {chat_id} unmuted successfully."
        except Exception as e:
            return log_and_format_error("unmute_chat", e, chat_id=chat_id)

    # ==================== Archive Operations ====================

    async def archive_chat(self, chat_id: Union[int, str]) -> str:
        """Archive a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            await self.client(
                functions.folders.EditPeerFoldersRequest(
                    folder_peers=[functions.InputFolderPeer(peer=entity, folder_id=1)]
                )
            )
            return f"Chat {chat_id} archived successfully."
        except Exception as e:
            return log_and_format_error("archive_chat", e, chat_id=chat_id)

    async def unarchive_chat(self, chat_id: Union[int, str]) -> str:
        """Unarchive a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            entity = await self.client.get_entity(chat_id)
            await self.client(
                functions.folders.EditPeerFoldersRequest(
                    folder_peers=[functions.InputFolderPeer(peer=entity, folder_id=0)]
                )
            )
            return f"Chat {chat_id} unarchived successfully."
        except Exception as e:
            return log_and_format_error("unarchive_chat", e, chat_id=chat_id)

    # ==================== Draft Operations ====================

    async def save_draft(
        self, chat_id: Union[int, str], message: str, reply_to: Optional[int] = None
    ) -> str:
        """Save a draft message to a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            peer = await self.client.get_input_entity(chat_id)
            await self.client(
                functions.messages.SaveDraftRequest(
                    peer=peer, message=message, reply_to_msg_id=reply_to
                )
            )
            return f"Draft saved to chat {chat_id}."
        except Exception as e:
            return log_and_format_error("save_draft", e, chat_id=chat_id)

    async def clear_draft(self, chat_id: Union[int, str]) -> str:
        """Clear a draft from a chat."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            peer = await self.client.get_input_entity(chat_id)
            await self.client(functions.messages.SaveDraftRequest(peer=peer, message=""))
            return f"Draft cleared from chat {chat_id}."
        except Exception as e:
            return log_and_format_error("clear_draft", e, chat_id=chat_id)

    # ==================== Media Operations ====================

    async def download_media(
        self, chat_id: Union[int, str], message_id: int, output_path: Optional[str] = None
    ) -> str:
        """Download media from a message."""
        chat_id, error = validate_ids("chat_id", chat_id)
        if error:
            return error

        try:
            # await self._wait_for_rate_limit()  # Закомментировано для отладки
            try:
                entity = await self.client.get_entity(chat_id)
            except TelethonFloodWaitError as e:
                wait_time = min(float(getattr(e, 'seconds', 0)), 3600.0)
                if wait_time > 0:
                    await asyncio.sleep(wait_time + random.uniform(0, 1))
                    entity = await self.client.get_entity(chat_id)
                else:
                    raise
            
            # await self._wait_for_rate_limit()  # Закомментировано для отладки
            try:
                message = await self.client.get_messages(entity, ids=message_id)
            except TelethonFloodWaitError as e:
                wait_time = min(float(getattr(e, 'seconds', 0)), 3600.0)
                if wait_time > 0:
                    await asyncio.sleep(wait_time + random.uniform(0, 1))
                    message = await self.client.get_messages(entity, ids=message_id)
                else:
                    raise
            
            if not message:
                return json.dumps({"success": False, "error": "Message not found"})
            
            if not message.media:
                return json.dumps({"success": False, "error": "Message has no media"})
            
            if not output_path:
                # Дефолтный путь в доступную директорию
                default_dir = "/home/eyurc/clawd/media"
                os.makedirs(default_dir, exist_ok=True)
                ext = ".ogg"
                if hasattr(message.media, 'document') and message.media.document:
                    for attr in message.media.document.attributes:
                        if hasattr(attr, 'file_name') and attr.file_name:
                            ext = os.path.splitext(attr.file_name)[1] or ext
                            break
                output_path = os.path.join(default_dir, f"tg_media_{chat_id}_{message_id}{ext}")
            else:
                # Если путь указан, убедимся что директория существует
                output_dir = os.path.dirname(output_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
            
            downloaded_path = await self.client.download_media(message, output_path)
            
            if downloaded_path:
                return json.dumps({"success": True, "path": downloaded_path})
            else:
                return json.dumps({"success": False, "error": "Failed to download"})
                
        except Exception as e:
            return log_and_format_error("download_media", e, chat_id=chat_id, message_id=message_id)


# Global singleton instance
telegram = TelegramCore()
