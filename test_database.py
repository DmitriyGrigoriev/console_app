import unittest
from unittest.mock import MagicMock, patch
from io import StringIO
from database import Database, EventBus, ConsoleInterface, Event

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

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.bus = EventBus()
        self.db = Database(self.bus)
        self.output_mock = MagicMock()
        self.bus.subscribe("OUTPUT", self.output_mock)

    def test_handle_set_get(self):
        self.bus.publish(Event("SET", ["key1", "value1"]))
        self.bus.publish(Event("GET", "key1"))
        self.output_mock.assert_called_with(Event("OUTPUT", "value1"))

    def test_handle_unset(self):
        self.bus.publish(Event("SET", ["temp", "data"]))
        self.bus.publish(Event("UNSET", "temp"))
        self.bus.publish(Event("GET", "temp"))
        self.output_mock.assert_called_with(Event("OUTPUT", "NULL"))

    def test_transaction_flow(self):
        self.bus.publish(Event("BEGIN", None))
        self.bus.publish(Event("SET", ["x", "10"]))
        self.bus.publish(Event("BEGIN", None))
        self.bus.publish(Event("SET", ["x", "20"]))
        self.bus.publish(Event("ROLLBACK", None))
        self.bus.publish(Event("GET", "x"))
        self.output_mock.assert_called_with(Event("OUTPUT", "10"))

    def test_counts_value(self):
        self.bus.publish(Event("SET", ["a", "10"]))
        self.bus.publish(Event("SET", ["b", "20"]))
        self.bus.publish(Event("SET", ["c", "10"]))
        self.bus.publish(Event("COUNTS", "10"))
        self.output_mock.assert_called_with(Event("OUTPUT", 2))

    def test_unknown_command(self):
        self.bus.publish(Event("INPUT", "INVALID CMD"))
        self.output_mock.assert_called_with(Event("OUTPUT", "Unknown command or invalid arguments"))

class TestConsoleInterface(unittest.TestCase):
    def setUp(self):
        self.bus = EventBus()
        self.console = ConsoleInterface(self.bus)
        self.bus.subscribe("SHUTDOWN", MagicMock())

    # @patch('builtins.input', return_value="SET x 10")
    # def test_input_handling(self, mock_input):
    #     with patch.object(self.bus, 'publish') as mock_publish:
    #         self.console.start()
    #         mock_publish.assert_called_with(Event("INPUT", "SET x 10"))
    #
    # @patch('builtins.input', return_value="END")
    # def test_shutdown(self, mock_input):
    #     self.console.start()
    #     self.assertFalse(self.console.running)

    @patch('sys.stdout', new_callable=StringIO)
    def test_output_handling(self, mock_stdout):
        self.bus.publish(Event("OUTPUT", "test output"))
        printed = mock_stdout.getvalue().strip()
        self.assertEqual(printed, "test output")

class TestIntegration(unittest.TestCase):
    @patch('builtins.input', side_effect=["SET name Alice", "GET name", "END"])
    @patch('sys.stdout', new_callable=StringIO)
    def test_full_flow(self, mock_stdout:MagicMock, mock_input:MagicMock):
        from database import main
        main()
        output = mock_stdout.getvalue().strip().split('\n')
        self.assertIn("Alice", output[-1])

    @patch('builtins.input', side_effect=["BEGIN", "SET balance 100", "ROLLBACK", "GET balance", "END"])
    @patch('sys.stdout', new_callable=StringIO)
    def test_transaction_flow(self, mock_stdout:MagicMock, mock_input:MagicMock):
        from database import main
        main()
        output = mock_stdout.getvalue().strip().split('\n')
        self.assertIn("NULL", output[-1])

if __name__ == '__main__':
    unittest.main()
