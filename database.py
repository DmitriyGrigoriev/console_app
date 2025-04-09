from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


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

    Maintains a registry of subscribers and allows publishing events to all subscribers.
    """

    def __init__(self) -> None:
        """Initializes the EventBus with an empty subscribers dictionary."""
        self._subscribers: dict[str, list[Callable]] = {}

    def subscribe(self, event_name: str, callback: Callable) -> None:
        """Subscribes a callback function to a specific event.

        Args:
            event_name (str): Name of the event to subscribe to.
            callback (Callable): Function to be called when event is published.
        """
        if event_name not in self._subscribers:
            self._subscribers[event_name] = []
        self._subscribers[event_name].append(callback)

    def publish(self, event: Event) -> None:
        """Publishes an event to all subscribed callbacks.

        Args:
            event (Event): The event to publish.
        """
        if event.name in self._subscribers:
            for callback in self._subscribers[event.name]:
                callback(event)


class Database:
    """In-memory database with transaction support.

    Handles various database commands and maintains transaction state.
    """

    def __init__(self, event_bus: EventBus) -> None:
        """Initializes the Database with event bus and command handlers.

        Args:
            event_bus (EventBus): Event bus for command processing.
        """
        self.store = {}
        self.transaction_stack = []
        self.transaction_data = []
        self.event_bus = event_bus

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
        """Handles SET command to store key-value pairs.

        Args:
            event (Event): Event containing key and value as data.
        """
        key, value = event.data
        if self.transaction_stack:
            self.transaction_data[-1][key] = self.store.get(key, None)
        self.store[key] = value

    def _handle_get(self, event: Event) -> None:
        """Handles GET command to retrieve a value by key.

        Args:
            event (Event): Event containing the key to retrieve.
        """
        key = event.data
        result = self.store.get(key, "NULL")
        self.event_bus.publish(Event("OUTPUT", result))

    def _handle_unset(self, event: Event) -> None:
        """Handles UNSET command to remove a key-value pair.

        Args:
            event (Event): Event containing the key to remove.
        """
        key = event.data
        if key in self.store:
            if self.transaction_stack:
                self.transaction_data[-1][key] = self.store[key]
            del self.store[key]

    def _handle_counts(self, event: Event) -> None:
        """Handles COUNTS command to count occurrences of a value.

        Args:
            event (Event): Event containing the value to count.
        """
        value = event.data
        count = sum(1 for v in self.store.values() if v == value)
        self.event_bus.publish(Event("OUTPUT", count))

    def _handle_find(self, event: Event) -> None:
        """Handles FIND command to locate keys by value.

        Args:
            event (Event): Event containing the value to find.
        """
        value = event.data
        result = [k for k, v in self.store.items() if v == value]
        output = ' '.join(sorted(result)) if result else "NULL"
        self.event_bus.publish(Event("OUTPUT", output))

    def _handle_begin(self, event: Event) -> None:
        """Handles BEGIN command to start a new transaction."""
        self.transaction_stack.append(True)
        self.transaction_data.append({})

    def _handle_rollback(self, event: Event) -> None:
        """Handles ROLLBACK command to undo current transaction."""
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

    def _handle_commit(self, event: Event) -> None:
        """Handles COMMIT command to commit current transaction."""
        if self.transaction_stack:
            self.transaction_data.pop()
            self.transaction_stack.pop()
        else:
            self.event_bus.publish(Event("OUTPUT", "NO TRANSACTION"))

    def _process_input(self, event: Event) -> None:
        """Processes raw input and publishes corresponding command events.

        Args:
            event (Event): Event containing the input line to process.
        """
        line = event.data.strip()
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
    """Provides console interface for interacting with the database."""

    def __init__(self, event_bus: EventBus) -> None:
        """Initializes the console interface.

        Args:
            event_bus (EventBus): Event bus for communication.
        """
        self.event_bus = event_bus
        self.running = True

        self.event_bus.subscribe("OUTPUT", self._handle_output)
        self.event_bus.subscribe("SHUTDOWN", self._handle_shutdown)

    def _handle_output(self, event: Event) -> None:
        """Handles OUTPUT events by printing to console.

        Args:
            event (Event): Event containing data to output.
        """
        print(event.data)

    def _handle_shutdown(self, event: Event) -> None:
        """Handles SHUTDOWN event by stopping the interface."""
        self.running = False

    def start(self) -> None:
        """Starts the console interface and processes user input."""
        print("Enter commands (type 'END' to exit):")
        while self.running:
            try:
                line = input("> ")
                self.event_bus.publish(Event("INPUT", line))
            except EOFError:
                self.event_bus.publish(Event("SHUTDOWN"))


def main() -> None:
    """Main entry point that initializes and starts the application."""
    event_bus = EventBus()
    Database(event_bus)
    console = ConsoleInterface(event_bus)

    console.start()


if __name__ == "__main__":
    main()
