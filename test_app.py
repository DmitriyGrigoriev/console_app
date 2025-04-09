import unittest
from unittest.mock import MagicMock, patch
from io import StringIO
from app import EventBus, InMemoryDatabase, RedisDatabase, DatabaseCommandHandler, ConsoleInterface, Event


class TestEventBus(unittest.TestCase):
    def setUp(self):
        self.bus = EventBus()

    def test_subscribe_publish(self):
        mock_callback = MagicMock()
        self.bus.subscribe("test_event", mock_callback)
        self.bus.publish(Event("test_event", "data"))
        mock_callback.assert_called_once()

    def test_multiple_subscribers(self):
        mock1 = MagicMock()
        mock2 = MagicMock()
        self.bus.subscribe("multi", mock1)
        self.bus.subscribe("multi", mock2)
        self.bus.publish(Event("multi", None))
        mock1.assert_called_once()
        mock2.assert_called_once()


class TestDatabaseCommandHandler(unittest.TestCase):
    def setUp(self):
        self.bus = EventBus()
        self.db_mock = MagicMock()
        self.handler = DatabaseCommandHandler(self.bus, self.db_mock)

    def test_set_command(self):
        self.bus.publish(Event("SET", ["key", "value"]))
        self.db_mock.handle_set.assert_called_once_with("key", "value")

    def test_get_command(self):
        self.bus.publish(Event("GET", "key"))
        self.db_mock.handle_get.assert_called_once_with("key")

    def test_unset_command(self):
        self.bus.publish(Event("UNSET", "key"))
        self.db_mock.handle_unset.assert_called_once_with("key")

    def test_counts_command(self):
        self.bus.publish(Event("COUNTS", "value"))
        self.db_mock.handle_counts.assert_called_once_with("value")

    def test_find_command(self):
        self.bus.publish(Event("FIND", "value"))
        self.db_mock.handle_find.assert_called_once_with("value")

    def test_begin_command(self):
        self.bus.publish(Event("BEGIN", None))
        self.db_mock.handle_begin.assert_called_once()

    def test_rollback_command(self):
        self.bus.publish(Event("ROLLBACK", None))
        self.db_mock.handle_rollback.assert_called_once()

    def test_commit_command(self):
        self.bus.publish(Event("COMMIT", None))
        self.db_mock.handle_commit.assert_called_once()

    def test_input_processing(self):
        self.bus.publish(Event("INPUT", "SET x 10"))
        self.db_mock.process_input.assert_called_once_with("SET x 10")


class TestInMemoryDatabase(unittest.TestCase):
    def setUp(self):
        self.bus = EventBus()
        self.db = InMemoryDatabase(self.bus)
        self.output_mock = MagicMock()
        self.bus.subscribe("OUTPUT", self.output_mock)

    def test_handle_set_get(self):
        self.db.handle_set("key1", "value1")
        self.db.handle_get("key1")
        self.output_mock.assert_called_with(Event("OUTPUT", "value1"))

    def test_handle_unset(self):
        self.db.handle_set("temp", "data")
        self.db.handle_unset("temp")
        self.db.handle_get("temp")
        self.output_mock.assert_called_with(Event("OUTPUT", "NULL"))

    def test_transaction_flow(self):
        self.db.handle_begin()
        self.db.handle_set("x", "10")
        self.db.handle_begin()
        self.db.handle_set("x", "20")
        self.db.handle_rollback()
        self.db.handle_get("x")
        self.output_mock.assert_called_with(Event("OUTPUT", "10"))

    def test_counts_value(self):
        self.db.handle_set("a", "10")
        self.db.handle_set("b", "20")
        self.db.handle_set("c", "10")
        self.db.handle_counts("10")
        self.output_mock.assert_called_with(Event("OUTPUT", 2))

    def test_find_value(self):
        self.db.handle_set("a", "findme")
        self.db.handle_set("b", "ignore")
        self.db.handle_set("c", "findme")
        self.db.handle_find("findme")
        output = self.output_mock.call_args[0][0].data
        self.assertIn("a", output)
        self.assertIn("c", output)

    # def test_process_input(self):
    #     self.db.process_input("SET x 10")
    #     self.db.handle_get("x")
    #     self.output_mock.assert_called_with(Event("OUTPUT", "10"))


class TestRedisDatabase(unittest.TestCase):
    def setUp(self):
        self.bus = EventBus()
        self.db = RedisDatabase(self.bus)
        self.output_mock = MagicMock()
        self.bus.subscribe("OUTPUT", self.output_mock)
        # Clear Redis for testing
        self.db.redis.flushdb()

    def tearDown(self):
        self.db.redis.flushdb()

    def test_handle_set_get(self):
        self.db.handle_set("key1", "value1")
        self.db.handle_get("key1")
        self.output_mock.assert_called_with(Event("OUTPUT", "value1"))

    def test_handle_unset(self):
        self.db.handle_set("temp", "data")
        self.db.handle_unset("temp")
        self.db.handle_get("temp")
        self.output_mock.assert_called_with(Event("OUTPUT", "NULL"))

    # def test_process_input(self):
    #     self.db.process_input("SET x 10")
    #     self.db.handle_get("x")
    #     self.output_mock.assert_called_with(Event("OUTPUT", "10"))


class TestConsoleInterface(unittest.TestCase):
    def setUp(self):
        self.bus = EventBus()
        self.console = ConsoleInterface(self.bus)
        self.bus.subscribe("SHUTDOWN", MagicMock())

    @patch('sys.stdout', new_callable=StringIO)
    def test_output_handling(self, mock_stdout):
        self.bus.publish(Event("OUTPUT", "test output"))
        printed = mock_stdout.getvalue().strip()
        self.assertEqual(printed, "test output")

    def test_shutdown(self):
        self.console._handle_shutdown(None)
        self.assertFalse(self.console.running)


class TestIntegration(unittest.TestCase):
    @patch('builtins.input', side_effect=["SET name Alice", "GET name", "END"])
    @patch('sys.stdout', new_callable=StringIO)
    def test_full_flow(self, mock_stdout, mock_input):
        from app import main
        main()
        output = mock_stdout.getvalue().strip().split('\n')
        self.assertIn("Alice", output[-1])

    @patch('builtins.input', side_effect=["BEGIN", "SET balance 100", "ROLLBACK", "GET balance", "END"])
    @patch('sys.stdout', new_callable=StringIO)
    def test_transaction_flow(self, mock_stdout, mock_input):
        from app import main
        main()
        output = mock_stdout.getvalue().strip().split('\n')
        self.assertIn("NULL", output[-1])


if __name__ == '__main__':
    unittest.main()
