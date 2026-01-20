"""
Telegram Client - Python client library for local scripts.

This client connects to the Telegram HTTP API running in Docker
and provides a simple interface for Telegram operations.

Usage:
    from telegram_client import TelegramClient

    client = TelegramClient()  # Defaults to http://localhost:8080

    # Get chats
    chats = client.get_chats()

    # Send a message
    client.send_message(chat_id=123456789, message="Hello!")
"""

import json
import time
import random
import re
from typing import Optional, List, Union, Any, Dict
from datetime import datetime, timedelta
import httpx


class TelegramClientError(Exception):
    """Exception raised for Telegram API errors."""
    pass


class RateLimitError(Exception):
    """Exception raised when rate limit is exceeded."""
    def __init__(self, message: str, retry_after: float = 1.0):
        super().__init__(message)
        self.retry_after = retry_after


class FloodWaitError(Exception):
    """Exception raised when Telegram requires waiting (FLOOD_WAIT)."""
    def __init__(self, message: str, wait_time: float = 1.0):
        super().__init__(message)
        self.wait_time = wait_time
        self.retry_after = wait_time  # Для совместимости с RateLimitError


class TelegramClient:
    """Client for interacting with the Telegram HTTP API."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        timeout: float = 30.0,
        min_request_delay: float = 0.2,  # Минимальная задержка между запросами (5 req/s)
        max_retries: int = 3,
    ):
        """
        Initialize the Telegram client.

        Args:
            base_url: The base URL of the Telegram API server.
            timeout: Request timeout in seconds.
            min_request_delay: Минимальная задержка между запросами в секундах (по умолчанию 0.2 = 5 req/s).
            max_retries: Максимальное количество повторных попыток при ошибках.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.min_request_delay = min_request_delay
        self.max_retries = max_retries
        self._client = httpx.Client(timeout=timeout)
        
        # Отслеживание времени последнего запроса
        self._last_request_time: Optional[float] = None
        
        # Отслеживание времени последнего сообщения в каждом чате (для лимита 1 msg/s)
        self._last_message_time_per_chat: Dict[Union[int, str], float] = {}
        
        # Отслеживание времени последнего редактирования (лимит 5 edits/s, 120 edits/hour)
        self._last_edit_time: Optional[float] = None
        self._edit_count_last_hour: int = 0
        self._edit_count_reset_time: Optional[float] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Close the HTTP client."""
        self._client.close()

    def _wait_for_rate_limit(self):
        """Ожидание перед следующим запросом для соблюдения rate limits."""
        current_time = time.time()
        
        if self._last_request_time is not None:
            elapsed = current_time - self._last_request_time
            if elapsed < self.min_request_delay:
                sleep_time = self.min_request_delay - elapsed
                # Добавляем небольшой jitter для избежания синхронизации
                sleep_time += random.uniform(0, 0.05)
                time.sleep(sleep_time)
        
        self._last_request_time = time.time()

    def _extract_flood_wait_time(self, error_msg: str, result: Dict) -> float:
        """
        Извлечение времени ожидания из FLOOD_WAIT ошибки.
        
        Args:
            error_msg: Текст ошибки
            result: Полный ответ API
            
        Returns:
            Время ожидания в секундах
        """
        wait_time = 1.0  # По умолчанию 1 секунда
        
        # Пытаемся извлечь из parameters
        parameters = result.get("parameters", {})
        if isinstance(parameters, dict):
            seconds = parameters.get("seconds") or parameters.get("retry_after")
            if seconds:
                wait_time = float(seconds)
        
        # Пытаемся извлечь из текста ошибки (формат: FLOOD_WAIT_123)
        match = re.search(r'FLOOD_WAIT[_\s]+(\d+)', str(error_msg).upper())
        if match:
            wait_time = float(match.group(1))
        
        # Ограничиваем максимальное время ожидания 3600 секундами (1 час)
        wait_time = min(wait_time, 3600.0)
        
        return wait_time

    def _handle_rate_limit_error(self, response: httpx.Response, attempt: int) -> float:
        """
        Обработка ошибки 429 (Rate Limit).
        
        Returns:
            Время ожидания до следующей попытки в секундах.
        """
        retry_after = 1.0  # По умолчанию 1 секунда
        
        try:
            # Пытаемся получить retry_after из ответа
            error_data = response.json()
            if isinstance(error_data, dict):
                # Может быть в разных форматах
                retry_after = error_data.get("retry_after", error_data.get("parameters", {}).get("retry_after", 1.0))
                if isinstance(retry_after, (int, float)):
                    retry_after = float(retry_after)
                else:
                    retry_after = 1.0
        except (ValueError, KeyError, TypeError):
            pass
        
        # Проверяем заголовки
        if "Retry-After" in response.headers:
            try:
                retry_after = float(response.headers["Retry-After"])
            except (ValueError, TypeError):
                pass
        
        # Exponential backoff с jitter
        backoff_time = retry_after * (2 ** attempt)
        # Добавляем jitter (0-25% от времени ожидания)
        jitter = random.uniform(0, backoff_time * 0.25)
        total_wait = backoff_time + jitter
        
        # Ограничиваем максимальное время ожидания 60 секундами
        total_wait = min(total_wait, 60.0)
        
        return total_wait

    def _check_edit_rate_limit(self):
        """Проверка лимита редактирования (5 edits/s, 120 edits/hour)."""
        current_time = time.time()
        
        # Сброс счетчика каждый час
        if self._edit_count_reset_time is None or current_time >= self._edit_count_reset_time:
            self._edit_count_last_hour = 0
            self._edit_count_reset_time = current_time + 3600
        
        # Проверка лимита 120 редактирований в час
        if self._edit_count_last_hour >= 120:
            wait_time = self._edit_count_reset_time - current_time
            if wait_time > 0:
                raise RateLimitError(
                    f"Edit rate limit exceeded: 120 edits/hour. Wait {wait_time:.1f} seconds.",
                    retry_after=wait_time
                )
        
        # Проверка лимита 5 редактирований в секунду
        if self._last_edit_time is not None:
            elapsed = current_time - self._last_edit_time
            if elapsed < 0.2:  # 5 edits/s = 1 edit per 0.2s
                sleep_time = 0.2 - elapsed + random.uniform(0, 0.02)
                time.sleep(sleep_time)
        
        self._last_edit_time = time.time()
        self._edit_count_last_hour += 1

    def _check_message_rate_limit(self, chat_id: Union[int, str]):
        """Проверка лимита отправки сообщений (1 msg/s в один чат)."""
        current_time = time.time()
        
        if chat_id in self._last_message_time_per_chat:
            elapsed = current_time - self._last_message_time_per_chat[chat_id]
            if elapsed < 1.0:  # Минимум 1 секунда между сообщениями в один чат
                sleep_time = 1.0 - elapsed + random.uniform(0, 0.1)
                time.sleep(sleep_time)
        
        self._last_message_time_per_chat[chat_id] = time.time()

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        check_message_rate_limit: bool = False,
        chat_id: Optional[Union[int, str]] = None,
        is_edit: bool = False,
    ) -> Any:
        """
        Make an HTTP request to the API with rate limiting protection.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: URL parameters
            json_data: JSON body data
            check_message_rate_limit: Проверять лимит сообщений для чата
            chat_id: ID чата для проверки лимита сообщений
            is_edit: Является ли запрос редактированием сообщения
        """
        # Проверка лимита редактирования
        if is_edit:
            self._check_edit_rate_limit()
        
        # Проверка лимита сообщений для конкретного чата
        if check_message_rate_limit and chat_id is not None:
            self._check_message_rate_limit(chat_id)
        
        # Ожидание перед запросом для соблюдения общего rate limit
        self._wait_for_rate_limit()
        
        url = f"{self.base_url}{endpoint}"
        
        # Повторные попытки с exponential backoff
        last_exception = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                )
                
                # Обработка ошибки 429 (Rate Limit)
                if response.status_code == 429:
                    wait_time = self._handle_rate_limit_error(response, attempt)
                    
                    if attempt < self.max_retries:
                        time.sleep(wait_time)
                        continue
                    else:
                        raise RateLimitError(
                            f"Rate limit exceeded after {self.max_retries} retries. "
                            f"Wait {wait_time:.1f} seconds before next request.",
                            retry_after=wait_time
                        )
                
                response.raise_for_status()
                
                result = response.json()
                
                if not result.get("success"):
                    error_msg = result.get("error", "Unknown error")
                    error_code = result.get("error_code", "")
                    
                    # Обработка FLOOD_WAIT ошибки
                    if "FLOOD_WAIT" in str(error_code).upper() or "FLOOD_WAIT" in str(error_msg).upper():
                        wait_time = self._extract_flood_wait_time(error_msg, result)
                        
                        if attempt < self.max_retries:
                            # Автоматически ждем и повторяем
                            time.sleep(wait_time + random.uniform(0, 1))  # Добавляем jitter
                            continue
                        else:
                            raise FloodWaitError(
                                f"Flood wait required: {error_msg}. "
                                f"Wait {wait_time:.1f} seconds before next request.",
                                wait_time=wait_time
                            )
                    
                    raise TelegramClientError(error_msg)
                
                data = result.get("data")
                if data:
                    try:
                        return json.loads(data)
                    except (json.JSONDecodeError, TypeError):
                        return data
                return data
                
            except FloodWaitError as e:
                # FloodWaitError уже обработан выше, но если дошли сюда - все попытки исчерпаны
                raise
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    # Уже обработано выше
                    continue
                last_exception = e
                if attempt < self.max_retries:
                    # Exponential backoff для других HTTP ошибок
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
                    continue
                raise
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_exception = e
                if attempt < self.max_retries:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(wait_time)
                    continue
                raise
        
        # Если дошли сюда, значит все попытки исчерпаны
        if last_exception:
            raise last_exception
        raise TelegramClientError("Request failed after all retries")

    def _get(self, endpoint: str, **params) -> Any:
        """Make a GET request."""
        return self._request("GET", endpoint, params=params)

    def _post(self, endpoint: str, check_message_rate_limit: bool = False, chat_id: Optional[Union[int, str]] = None, **data) -> Any:
        """Make a POST request."""
        return self._request(
            "POST",
            endpoint,
            json_data=data,
            check_message_rate_limit=check_message_rate_limit,
            chat_id=chat_id
        )

    def _put(self, endpoint: str, is_edit: bool = False, **data) -> Any:
        """Make a PUT request."""
        return self._request("PUT", endpoint, json_data=data, is_edit=is_edit)

    def _delete(self, endpoint: str, **data) -> Any:
        """Make a DELETE request."""
        if data:
            return self._request("DELETE", endpoint, json_data=data)
        return self._request("DELETE", endpoint)

    # ==================== Health Check ====================

    def health_check(self) -> Dict:
        """Check if the API is healthy."""
        response = self._client.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()

    # ==================== Chat Operations ====================

    def get_chats(self, page: int = 1, page_size: int = 20) -> str:
        """Get a paginated list of chats."""
        return self._get("/chats", page=page, page_size=page_size)

    def list_chats(
        self,
        limit: int = 50,
        chat_type: Optional[str] = None,
        archived: bool = False,
        unread_only: bool = False,
    ) -> List[Dict]:
        """Get a filtered list of chats with metadata."""
        params = {"limit": limit, "archived": archived, "unread_only": unread_only}
        if chat_type:
            params["chat_type"] = chat_type
        return self._get("/chats/list", **params)

    def get_chat(self, chat_id: Union[int, str]) -> Dict:
        """Get detailed information about a specific chat."""
        return self._get(f"/chats/{chat_id}")

    # ==================== Message Operations ====================

    def get_messages(
        self, chat_id: Union[int, str], page: int = 1, page_size: int = 20
    ) -> str:
        """Get paginated messages from a chat."""
        return self._get(f"/chats/{chat_id}/messages", page=page, page_size=page_size)

    def send_message(
        self,
        chat_id: Union[int, str],
        message: str,
        reply_to: Optional[int] = None,
        parse_mode: Optional[str] = None,
    ) -> str:
        """Send a message to a chat with rate limiting protection."""
        data = {"chat_id": chat_id, "message": message}
        if reply_to:
            data["reply_to"] = reply_to
        if parse_mode:
            data["parse_mode"] = parse_mode
        return self._post(
            "/messages/send",
            **data,
            check_message_rate_limit=True,
            chat_id=chat_id
        )

    def edit_message(
        self, chat_id: Union[int, str], message_id: int, new_text: str
    ) -> str:
        """Edit an existing message with rate limiting protection."""
        return self._put(
            "/messages/edit",
            chat_id=chat_id,
            message_id=message_id,
            new_text=new_text,
            is_edit=True
        )

    def delete_message(
        self, chat_id: Union[int, str], message_id: int, revoke: bool = True
    ) -> str:
        """Delete a message."""
        return self._delete(
            "/messages/delete",
            chat_id=chat_id,
            message_id=message_id,
            revoke=revoke,
        )

    def forward_message(
        self, from_chat_id: Union[int, str], to_chat_id: Union[int, str], message_id: int
    ) -> str:
        """Forward a message from one chat to another with rate limiting protection."""
        return self._post(
            "/messages/forward",
            from_chat_id=from_chat_id,
            to_chat_id=to_chat_id,
            message_id=message_id,
            check_message_rate_limit=True,
            chat_id=to_chat_id  # Лимит применяется к целевому чату
        )

    def search_messages(
        self,
        chat_id: Union[int, str],
        query: str,
        limit: int = 20,
        from_user: Optional[Union[int, str]] = None,
    ) -> List[Dict]:
        """Search for messages in a chat."""
        data = {"chat_id": chat_id, "query": query, "limit": limit}
        if from_user:
            data["from_user"] = from_user
        return self._post("/messages/search", **data)

    # ==================== Contact Operations ====================

    def list_contacts(self) -> List[Dict]:
        """Get all contacts."""
        return self._get("/contacts")

    def search_contacts(self, query: str, limit: int = 10) -> List[Dict]:
        """Search contacts by name or username."""
        return self._get("/contacts/search", query=query, limit=limit)

    def add_contact(
        self, phone: str, first_name: str, last_name: Optional[str] = None
    ) -> str:
        """Add a new contact."""
        data = {"phone": phone, "first_name": first_name}
        if last_name:
            data["last_name"] = last_name
        return self._post("/contacts", **data)

    def delete_contact(self, user_id: Union[int, str]) -> str:
        """Delete a contact."""
        return self._delete(f"/contacts/{user_id}")

    # ==================== User Operations ====================

    def get_me(self) -> Dict:
        """Get information about the current user."""
        return self._get("/me")

    def get_user_status(self, user_id: Union[int, str]) -> Dict:
        """Get the online status of a user."""
        return self._get(f"/users/{user_id}/status")

    def resolve_username(self, username: str) -> Dict:
        """Resolve a username to get entity information."""
        return self._get(f"/resolve/{username}")

    # ==================== Group Operations ====================

    def create_group(self, title: str, users: List[Union[int, str]]) -> str:
        """Create a new group chat."""
        return self._post("/groups", title=title, users=users)

    def invite_to_group(
        self, chat_id: Union[int, str], user_ids: List[Union[int, str]]
    ) -> str:
        """Invite users to a group or channel."""
        return self._post("/groups/invite", chat_id=chat_id, user_ids=user_ids)

    def leave_chat(self, chat_id: Union[int, str]) -> str:
        """Leave a group or channel."""
        return self._post(f"/chats/{chat_id}/leave")

    def get_participants(
        self, chat_id: Union[int, str], limit: int = 100, offset: int = 0
    ) -> List[Dict]:
        """Get participants of a group or channel."""
        return self._get(f"/chats/{chat_id}/participants", limit=limit, offset=offset)

    # ==================== Admin Operations ====================

    def get_admins(self, chat_id: Union[int, str]) -> List[Dict]:
        """Get administrators of a chat."""
        return self._get(f"/chats/{chat_id}/admins")

    def promote_admin(
        self,
        chat_id: Union[int, str],
        user_id: Union[int, str],
        title: Optional[str] = None,
    ) -> str:
        """Promote a user to admin."""
        data = {"chat_id": chat_id, "user_id": user_id}
        if title:
            data["title"] = title
        return self._post("/admin/promote", **data)

    def ban_user(
        self,
        chat_id: Union[int, str],
        user_id: Union[int, str],
        until_date: Optional[int] = None,
    ) -> str:
        """Ban a user from a chat."""
        data = {"chat_id": chat_id, "user_id": user_id}
        if until_date:
            data["until_date"] = until_date
        return self._post("/admin/ban", **data)

    def unban_user(self, chat_id: Union[int, str], user_id: Union[int, str]) -> str:
        """Unban a user from a chat."""
        return self._post("/admin/unban", chat_id=chat_id, user_id=user_id)

    # ==================== Channel Operations ====================

    def get_invite_link(self, chat_id: Union[int, str]) -> str:
        """Get the invite link for a chat."""
        return self._get(f"/chats/{chat_id}/invite-link")

    # ==================== Notification Operations ====================

    def mute_chat(
        self, chat_id: Union[int, str], mute_until: Optional[int] = None
    ) -> str:
        """Mute notifications for a chat."""
        params = {}
        if mute_until:
            params["mute_until"] = mute_until
        return self._post(f"/chats/{chat_id}/mute", **params)

    def unmute_chat(self, chat_id: Union[int, str]) -> str:
        """Unmute notifications for a chat."""
        return self._post(f"/chats/{chat_id}/unmute")

    # ==================== Archive Operations ====================

    def archive_chat(self, chat_id: Union[int, str]) -> str:
        """Archive a chat."""
        return self._post(f"/chats/{chat_id}/archive")

    def unarchive_chat(self, chat_id: Union[int, str]) -> str:
        """Unarchive a chat."""
        return self._post(f"/chats/{chat_id}/unarchive")

    # ==================== Draft Operations ====================

    def save_draft(
        self, chat_id: Union[int, str], message: str, reply_to: Optional[int] = None
    ) -> str:
        """Save a draft message to a chat."""
        data = {"chat_id": chat_id, "message": message}
        if reply_to:
            data["reply_to"] = reply_to
        return self._post("/drafts/save", **data)

    def clear_draft(self, chat_id: Union[int, str]) -> str:
        """Clear a draft from a chat."""
        return self._delete(f"/drafts/{chat_id}")


# Convenience function for quick usage
def get_client(base_url: str = "http://localhost:8080") -> TelegramClient:
    """Get a Telegram client instance."""
    return TelegramClient(base_url=base_url)
