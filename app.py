from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol
import redis


@dataclass
class Event:
    """Represents an event with a name and associated data.

    Attributes:
        name (str): The name of the event.
        data (Any, optional): Additional data associated with the event. Defaults to None.
    """
    name: str
    data: Any = None


class EventBus:
    """Implements a publish-subscribe pattern for event handling.

    The EventBus allows components to subscribe to specific events and publish events
    to all subscribed callbacks.
    """

    def __init__(self) -> None:
        """Initialize the EventBus with an empty subscribers dictionary."""
        self._subscribers: dict[str, list[Callable]] = {}

    def subscribe(self, event_name: str, callback: Callable) -> None:
        """Subscribe a callback function to a specific event.

        Args:
            event_name (str): Name of the event to subscribe to.
            callback (Callable): Function to be called when the event is published.
        """
        if event_name not in self._subscribers:
            self._subscribers[event_name] = []
        self._subscribers[event_name].append(callback)

    def publish(self, event: Event) -> None:
        """Publish an event to all subscribed callbacks.

        Args:
            event (Event): The event to publish.
        """
        if event.name in self._subscribers:
            for callback in self._subscribers[event.name]:
                callback(event)


class DatabaseProtocol(Protocol):
    """Protocol defining the interface for database implementations.

    This protocol specifies the methods that any database implementation must provide
    to work with the DatabaseCommandHandler.
    """

    def handle_set(self, key: str, value: str) -> None:
        """Handle SET command to store a key-value pair.

        Args:
            key (str): The key to set.
            value (str): The value to associate with the key.
        """
        ...

    def handle_get(self, key: str) -> None:
        """Handle GET command to retrieve a value by key.

        Args:
            key (str): The key to retrieve.
        """
        ...

    def handle_unset(self, key: str) -> None:
        """Handle UNSET command to remove a key-value pair.

        Args:
            key (str): The key to remove.
        """
        ...

    def handle_counts(self, value: str) -> None:
        """Handle COUNTS command to count occurrences of a value.

        Args:
            value (str): The value to count.
        """
        ...

    def handle_find(self, value: str) -> None:
        """Handle FIND command to locate keys by value.

        Args:
            value (str): The value to find.
        """
        ...

    def handle_begin(self) -> None:
        """Handle BEGIN command to start a new transaction."""
        ...

    def handle_rollback(self) -> None:
        """Handle ROLLBACK command to undo the current transaction."""
        ...

    def handle_commit(self) -> None:
        """Handle COMMIT command to commit the current transaction."""
        ...

    def process_input(self, line: str) -> None:
        """Process raw input and publish corresponding command events.

        Args:
            line (str): The input line to process.
        """
        ...


class DatabaseCommandHandler:
    """Handles database commands using the injected database implementation.

    This class translates events into database operations using the provided
    database implementation that conforms to DatabaseProtocol.
    """

    def __init__(self, event_bus: EventBus, database: DatabaseProtocol) -> None:
        """Initialize the command handler with event bus and database.

        Args:
            event_bus (EventBus): The event bus to subscribe to.
            database (DatabaseProtocol): The database implementation to use.
        """
        self.event_bus = event_bus
        self.database = database

        # Register command handlers
        self.event_bus.subscribe("SET", self._handle_set)
        self.event_bus.subscribe("GET", self._handle_get)
        self.event_bus.subscribe("UNSET", self._handle_unset)
        self.event_bus.subscribe("COUNTS", self._handle_counts)
        self.event_bus.subscribe("FIND", self._handle_find)
        self.event_bus.subscribe("BEGIN", self._handle_begin)
        self.event_bus.subscribe("ROLLBACK", self._handle_rollback)
        self.event_bus.subscribe("COMMIT", self._handle_commit)
        self.event_bus.subscribe("INPUT", self._process_input)

    def _handle_set(self, event: Event) -> None:
        """Handle SET event by delegating to database implementation.

        Args:
            event (Event): Event containing key and value as data.
        """
        key, value = event.data
        self.database.handle_set(key, value)

    def _handle_get(self, event: Event) -> None:
        """Handle GET event by delegating to database implementation.

        Args:
            event (Event): Event containing the key to retrieve.
        """
        key = event.data
        self.database.handle_get(key)

    def _handle_unset(self, event: Event) -> None:
        """Handle UNSET event by delegating to database implementation.

        Args:
            event (Event): Event containing the key to remove.
        """
        key = event.data
        self.database.handle_unset(key)

    def _handle_counts(self, event: Event) -> None:
        """Handle COUNTS event by delegating to database implementation.

        Args:
            event (Event): Event containing the value to count.
        """
        value = event.data
        self.database.handle_counts(value)

    def _handle_find(self, event: Event) -> None:
        """Handle FIND event by delegating to database implementation.

        Args:
            event (Event): Event containing the value to find.
        """
        value = event.data
        self.database.handle_find(value)

    def _handle_begin(self, event: Event) -> None:
        """Handle BEGIN event by delegating to database implementation."""
        self.database.handle_begin()

    def _handle_rollback(self, event: Event) -> None:
        """Handle ROLLBACK event by delegating to database implementation."""
        self.database.handle_rollback()

    def _handle_commit(self, event: Event) -> None:
        """Handle COMMIT event by delegating to database implementation."""
        self.database.handle_commit()

    def _process_input(self, event: Event) -> None:
        """Process INPUT event containing raw user input.

        Args:
            event (Event): Event containing the input line.
        """
        line = event.data.strip()
        self.database.process_input(line)


class InMemoryDatabase:
    """In-memory database implementation.

    This implementation stores data in a Python dictionary and supports
    basic transaction operations.
    """

    def __init__(self, event_bus: EventBus) -> None:
        """Initialize the in-memory database.

        Args:
            event_bus (EventBus): The event bus for publishing output events.
        """
        self.store = {}
        self.transaction_stack = []
        self.transaction_data = []
        self.event_bus = event_bus

    def handle_set(self, key: str, value: str) -> None:
        """Handle SET command for in-memory storage.

        Args:
            key (str): The key to set.
            value (str): The value to associate with the key.
        """
        if self.transaction_stack:
            self.transaction_data[-1][key] = self.store.get(key, None)
        self.store[key] = value

    def handle_get(self, key: str) -> None:
        """Handle GET command for in-memory storage.

        Args:
            key (str): The key to retrieve.
        """
        result = self.store.get(key, "NULL")
        self.event_bus.publish(Event("OUTPUT", result))

    def handle_unset(self, key: str) -> None:
        """Handle UNSET command for in-memory storage.

        Args:
            key (str): The key to remove.
        """
        if key in self.store:
            if self.transaction_stack:
                self.transaction_data[-1][key] = self.store[key]
            del self.store[key]

    def handle_counts(self, value: str) -> None:
        """Handle COUNTS command for in-memory storage.

        Args:
            value (str): The value to count.
        """
        count = sum(1 for v in self.store.values() if v == value)
        self.event_bus.publish(Event("OUTPUT", count))

    def handle_find(self, value: str) -> None:
        """Handle FIND command for in-memory storage.

        Args:
            value (str): The value to find.
        """
        result = [k for k, v in self.store.items() if v == value]
        output = ' '.join(sorted(result)) if result else "NULL"
        self.event_bus.publish(Event("OUTPUT", output))

    def handle_begin(self) -> None:
        """Handle BEGIN command for in-memory storage."""
        self.transaction_stack.append(True)
        self.transaction_data.append({})

    def handle_rollback(self) -> None:
        """Handle ROLLBACK command for in-memory storage."""
        if self.transaction_stack:
            last_transaction = self.transaction_data.pop()
            for key in last_transaction:
                if last_transaction[key] is None:
                    del self.store[key]
                else:
                    self.store[key] = last_transaction[key]
            self.transaction_stack.pop()
        else:
            self.event_bus.publish(Event("OUTPUT", "NO TRANSACTION"))

    def handle_commit(self) -> None:
        """Handle COMMIT command for in-memory storage."""
        if self.transaction_stack:
            self.transaction_data.pop()
            self.transaction_stack.pop()
        else:
            self.event_bus.publish(Event("OUTPUT", "NO TRANSACTION"))

    def process_input(self, line: str) -> None:
        """Process raw input for in-memory storage.

        Args:
            line (str): The input line to process.
        """
        if not line:
            return

        parts = line.split()
        command = parts[0].upper()
        args = parts[1:]

        if command == "END":
            self.event_bus.publish(Event("SHUTDOWN"))
            return

        if command in {"SET", "GET", "UNSET", "COUNTS", "FIND",
                       "BEGIN", "ROLLBACK", "COMMIT"}:
            self.event_bus.publish(Event(command, args[0] if len(args) == 1 else args))
        else:
            self.event_bus.publish(Event("OUTPUT", "Unknown command or invalid arguments"))


class RedisDatabase:
    """Redis database implementation.

    This implementation uses Redis as the backing store and supports
    basic transaction operations.
    """

    def __init__(self, event_bus: EventBus,
                 host: str = 'localhost',
                 port: int = 6379,
                 db: int = 0,
                 password: str = None) -> None:
        """Initialize the Redis database connection.

        Args:
            event_bus (EventBus): The event bus for publishing output events.
            host (str): Redis server host. Defaults to 'localhost'.
            port (int): Redis server port. Defaults to 6379.
            db (int): Redis database number. Defaults to 0.
            password (str, optional): Redis password. Defaults to None.
        """
        self.redis = redis.StrictRedis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        self.transaction_stack = []
        self.transaction_data = []
        self.event_bus = event_bus

    def handle_set(self, key: str, value: str) -> None:
        """Handle SET command for Redis storage.

        Args:
            key (str): The key to set.
            value (str): The value to associate with the key.
        """
        if self.transaction_stack:
            old_value = self.redis.get(key)
            self.transaction_data[-1][key] = old_value
        self.redis.set(key, value)

    def handle_get(self, key: str) -> None:
        """Handle GET command for Redis storage.

        Args:
            key (str): The key to retrieve.
        """
        result = self.redis.get(key) or "NULL"
        self.event_bus.publish(Event("OUTPUT", result))

    def handle_unset(self, key: str) -> None:
        """Handle UNSET command for Redis storage.

        Args:
            key (str): The key to remove.
        """
        if self.redis.exists(key):
            if self.transaction_stack:
                old_value = self.redis.get(key)
                self.transaction_data[-1][key] = old_value
            self.redis.delete(key)

    def handle_counts(self, value: str) -> None:
        """Handle COUNTS command for Redis storage.

        Note: This implementation scans all keys and is not efficient for large databases.
        For production use, consider using Redis Search or maintaining a separate index.

        Args:
            value (str): The value to count.
        """
        count = 0
        for key in self.redis.scan_iter():
            if self.redis.get(key) == value:
                count += 1
        self.event_bus.publish(Event("OUTPUT", count))

    def handle_find(self, value: str) -> None:
        """Handle FIND command for Redis storage.

        Note: This implementation scans all keys and is not efficient for large databases.
        For production use, consider using Redis Search or maintaining a separate index.

        Args:
            value (str): The value to find.
        """
        result = []
        for key in self.redis.scan_iter():
            if self.redis.get(key) == value:
                result.append(key)
        output = ' '.join(sorted(result)) if result else "NULL"
        self.event_bus.publish(Event("OUTPUT", output))

    def handle_begin(self) -> None:
        """Handle BEGIN command for Redis storage."""
        self.transaction_stack.append(True)
        self.transaction_data.append({})

    def handle_rollback(self) -> None:
        """Handle ROLLBACK command for Redis storage."""
        if self.transaction_stack:
            last_transaction = self.transaction_data.pop()
            with self.redis.pipeline() as pipe:
                for key, value in last_transaction.items():
                    if value is None:
                        pipe.delete(key)
                    else:
                        pipe.set(key, value)
                pipe.execute()
            self.transaction_stack.pop()
        else:
            self.event_bus.publish(Event("OUTPUT", "NO TRANSACTION"))

    def handle_commit(self) -> None:
        """Handle COMMIT command for Redis storage."""
        if self.transaction_stack:
            self.transaction_data.pop()
            self.transaction_stack.pop()
        else:
            self.event_bus.publish(Event("OUTPUT", "NO TRANSACTION"))

    def process_input(self, line: str) -> None:
        """Process raw input for Redis storage.

        Args:
            line (str): The input line to process.
        """
        if not line:
            return

        parts = line.split()
        command = parts[0].upper()
        args = parts[1:]

        if command == "END":
            self.event_bus.publish(Event("SHUTDOWN"))
            return

        if command in {"SET", "GET", "UNSET", "COUNTS", "FIND",
                       "BEGIN", "ROLLBACK", "COMMIT"}:
            self.event_bus.publish(Event(command, args[0] if len(args) == 1 else args))
        else:
            self.event_bus.publish(Event("OUTPUT", "Unknown command or invalid arguments"))


class ConsoleInterface:
    """Provides console interface for interacting with the database.

    This class handles user input and output through the console.
    """

    def __init__(self, event_bus: EventBus) -> None:
        """Initialize the console interface.

        Args:
            event_bus (EventBus): The event bus to subscribe to.
        """
        self.event_bus = event_bus
        self.running = True
        self.event_bus.subscribe("OUTPUT", self._handle_output)
        self.event_bus.subscribe("SHUTDOWN", self._handle_shutdown)

    def _handle_output(self, event: Event) -> None:
        """Handle OUTPUT events by printing to console.

        Args:
            event (Event): Event containing data to output.
        """
        print(event.data)

    def _handle_shutdown(self, event: Event) -> None:
        """Handle SHUTDOWN event by stopping the interface.

        Args:
            event (Event): The shutdown event.
        """
        self.running = False

    def start(self) -> None:
        """Start the console interface and process user input."""
        print("Enter commands (type 'END' to exit):")
        while self.running:
            try:
                line = input("> ")
                self.event_bus.publish(Event("INPUT", line))
            except EOFError:
                self.event_bus.publish(Event("SHUTDOWN"))


def main(database_type: str = "memory") -> None:
    """Main entry point for the database application.

    Args:
        database_type (str, optional): Type of database to use ('memory' or 'redis').
                                      Defaults to "memory".
    """
    event_bus = EventBus()

    if database_type == "redis":
        # Configuration for Redis (could be moved to config file or environment variables)
        redis_config = {
            'host': 'localhost',
            'port': 6379,
            'db': 0,
            'password': None
        }
        db = RedisDatabase(event_bus, **redis_config)
    else:
        db = InMemoryDatabase(event_bus)

    # Command handler uses the injected database implementation
    DatabaseCommandHandler(event_bus, db)
    console = ConsoleInterface(event_bus)
    console.start()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Database application with in-memory or Redis storage."
    )
    parser.add_argument("--database", choices=["memory", "redis"],
                        default="memory", help="Database type to use")
    args = parser.parse_args()

    main(database_type=args.database)