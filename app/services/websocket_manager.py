"""
WebSocket connection manager for live match updates.

Manages connections per game and broadcasts events to all connected clients.
"""
import asyncio
import logging
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages WebSocket connections for live match updates.

    Clients connect to specific games and receive real-time event broadcasts.
    """

    def __init__(self):
        # game_id -> list of websocket connections
        self.active_connections: dict[str, list[WebSocket]] = {}
        # Lock for thread-safe operations
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, game_id: str) -> None:
        """
        Accept a new WebSocket connection for a game.

        Args:
            websocket: The WebSocket connection
            game_id: Game UUID to subscribe to
        """
        await websocket.accept()

        async with self._lock:
            if game_id not in self.active_connections:
                self.active_connections[game_id] = []
            self.active_connections[game_id].append(websocket)

        logger.info(f"WebSocket connected for game {game_id}. Total connections: {len(self.active_connections.get(game_id, []))}")

    async def disconnect(self, websocket: WebSocket, game_id: str) -> None:
        """
        Remove a WebSocket connection.

        Args:
            websocket: The WebSocket connection to remove
            game_id: Game UUID the connection was subscribed to
        """
        async with self._lock:
            if game_id in self.active_connections:
                try:
                    self.active_connections[game_id].remove(websocket)
                except ValueError:
                    pass  # Connection not in list

                # Clean up empty game lists
                if not self.active_connections[game_id]:
                    del self.active_connections[game_id]

        logger.info(f"WebSocket disconnected for game {game_id}")

    async def broadcast_to_game(self, game_id: str, message: dict[str, Any]) -> int:
        """
        Broadcast a message to all connections for a specific game.

        Args:
            game_id: Game UUID to broadcast to
            message: JSON-serializable message to send

        Returns:
            Number of connections that received the message
        """
        connections = self.active_connections.get(game_id, [])
        if not connections:
            return 0

        sent_count = 0
        failed_connections = []

        for connection in connections:
            try:
                await connection.send_json(message)
                sent_count += 1
            except Exception as e:
                logger.warning(f"Failed to send to WebSocket: {e}")
                failed_connections.append(connection)

        # Clean up failed connections
        if failed_connections:
            async with self._lock:
                for conn in failed_connections:
                    try:
                        self.active_connections[game_id].remove(conn)
                    except (ValueError, KeyError):
                        pass

        return sent_count

    async def broadcast_event(self, game_id: str, event: dict[str, Any]) -> int:
        """
        Broadcast a match event to all connected clients.

        Args:
            game_id: Game UUID
            event: Event data dict

        Returns:
            Number of clients notified
        """
        message = {
            "type": "event",
            "game_id": game_id,
            "data": event,
        }
        return await self.broadcast_to_game(game_id, message)

    async def broadcast_lineup(self, game_id: str, lineup: dict[str, Any]) -> int:
        """
        Broadcast lineup update to all connected clients.

        Args:
            game_id: Game UUID
            lineup: Lineup data dict

        Returns:
            Number of clients notified
        """
        message = {
            "type": "lineup",
            "game_id": game_id,
            "data": lineup,
        }
        return await self.broadcast_to_game(game_id, message)

    async def broadcast_game_status(self, game_id: str, status: str) -> int:
        """
        Broadcast game status change (started, ended, etc.).

        Args:
            game_id: Game UUID
            status: Status string

        Returns:
            Number of clients notified
        """
        message = {
            "type": "status",
            "game_id": game_id,
            "status": status,
        }
        return await self.broadcast_to_game(game_id, message)

    def get_connection_count(self, game_id: str) -> int:
        """Get number of active connections for a game."""
        return len(self.active_connections.get(game_id, []))

    def get_all_game_ids(self) -> list[str]:
        """Get list of all game IDs with active connections."""
        return list(self.active_connections.keys())


# Global singleton instance
_manager: ConnectionManager | None = None


def get_websocket_manager() -> ConnectionManager:
    """Get the global WebSocket manager instance."""
    global _manager
    if _manager is None:
        _manager = ConnectionManager()
    return _manager
