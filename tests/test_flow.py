import unittest
from prefect.testing.utilities import prefect_test_harness
from flows.atp_calendar_flow import atp_calendar_etl

class TestATPFlow(unittest.TestCase):
    def test_flow_runs(self):
        with prefect_test_harness():
            result = atp_calendar_etl()
            self.assertIsNotNone(result)

if __name__ == "__main__":
    unittest.main()
