import unittest
import sys, os
import json

# Workaround to just import DataIngestor and the Job class
# without the __init__.py to start
SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
module_path = SCRIPT_DIR + '/app/'
sys.path.append(os.path.dirname(module_path))

from task_runner import Job
from data_ingestor import DataIngestor

class TestMeanMethods(unittest.TestCase):
    def setUp(self):
        self.csv_data = DataIngestor('nutrition_small.csv')
        # Since the csv is rather small I'll pair the first question with the first
        # state and the second one with the second state
        self.question_1 = "Percent of adults who engage in no leisure-time physical activity"
        self.question_2 = "Percent of adults who engage in muscle-strengthening activities on 2 or more days a week"
        self.state1 = "Ohio"
        self.state2 = "Rhode Island"
        # Since the id of the job doesn't matter i'll default to "job_id_1"
        self.job_id = "job_id_1"
    
    def test_states_mean(self):
        self.job = Job(self.job_id, "states_mean", self.csv_data)
        self.job.set_question(self.question_1)
        self.job.states_mean()
        dict_to_compare = {'Arizona': 25.6, 'Kansas': 28.7, 'Maryland': 38.4,
                           'Nebraska': 31.5, 'North Dakota': 17.8, 'Ohio': 31.6,
                           'Oklahoma': 31.3, 'Wisconsin': 24.0}
        self.assertEqual(self.job.result, dict_to_compare)
    
    def test_state_mean(self):
        self.job = Job(self.job_id, "state_mean", self.csv_data)
        self.job.set_question(self.question_1)
        self.job.set_state(self.state1)
        self.job.state_mean()
        dict_to_compare = {self.state1 : 31.6}
        self.assertEqual(self.job.result, dict_to_compare)

    def test_best5(self):
        self.job = Job(self.job_id, "best5", self.csv_data)
        self.job.set_question(self.question_2)
        self.job.best5()
        dict_to_compare = {"Washington": 40.3, "National": 38.7, "Vermont": 37.9, "New Hampshire": 35.3,
                           "Massachusetts": 31.4}
        self.assertEqual(self.job.result, dict_to_compare)

    def test_worst5(self):
        self.job = Job(self.job_id, "worst5", self.csv_data)
        self.job.set_question(self.question_1)
        self.job.best5()
        dict_to_compare = {"North Dakota": 17.8, "Wisconsin": 24.0, "Arizona": 25.6, "Kansas": 28.7, "Oklahoma": 31.3}
        self.assertEqual(self.job.result, dict_to_compare)

    def test_global_mean(self):
        self.job = Job(self.job_id, "global_mean", self.csv_data)
        self.job.set_question(self.question_1)
        self.job.global_mean()
        dict_to_compare = {"global_mean": 28.6125}
        self.assertEqual(self.job.result, dict_to_compare)

        self.job.set_question(self.question_2)
        self.job.global_mean()
        dict_to_compare = {"global_mean": 33.228571428571435}
        self.assertEqual(self.job.result, dict_to_compare)

    def test_diff_from_mean(self):
        self.job = Job(self.job_id, "diff_from_mean", self.csv_data)
        self.job.set_question(self.question_1)
        self.job.diff_from_mean()
        dict_to_compare = {"Arizona": 3.0124999999999993, "Kansas": -0.08749999999999858,
                           "Maryland": -9.787499999999998, "Nebraska": -2.8874999999999993,
                           "North Dakota": 10.8125, "Ohio": -2.9875000000000007,
                           "Oklahoma": -2.6875, "Wisconsin": 4.612500000000001}
        self.assertEqual(self.job.result, dict_to_compare)

    def test_state_diff_from_mean(self):
        self.job = Job(self.job_id, "state_diff_from_mean", self.csv_data)
        # First test of the first combo of question + state
        self.job.set_question(self.question_1)
        self.job.set_state(self.state1)
        self.job.state_diff_from_mean()
        dict_to_compare = {"Ohio": -2.9875000000000007}
        self.assertEqual(self.job.result, dict_to_compare)

        # Second test of the second combo of question + state
        self.job.set_question(self.question_2)
        self.job.set_state(self.state2)
        self.job.state_diff_from_mean()
        dict_to_compare = {"Rhode Island": 13.528571428571436}
        self.assertEqual(self.job.result, dict_to_compare)

    def test_mean_by_category(self):
        self.job = Job(self.job_id, "mean_by_category", self.csv_data)
        self.job.set_question(self.question_1)
        self.job.set_state(self.state1)
        self.job.mean_by_category()
        dict_to_compare = {"('Arizona', 'Race/Ethnicity', 'Non-Hispanic Black')": 25.6, "('Kansas', 'Age (years)', '45 - 54')": 28.7,
                           "('Maryland', 'Income', '$15,000 - $24,999')": 38.4, "('Nebraska', 'Income', '$15,000 - $24,999')": 31.5,
                           "('North Dakota', 'Race/Ethnicity', '2 or more races')": 17.8, "('Ohio', 'Race/Ethnicity', '2 or more races')": 31.6,
                           "('Oklahoma', 'Income', '$35,000 - $49,999')": 31.3, "('Wisconsin', 'Age (years)', '55 - 64')": 24.0}
        self.assertEqual(self.job.result, dict_to_compare)
    
    def test_state_mean_by_category(self):
        self.job = Job(self.job_id, "state_mean_by_category", self.csv_data)
        self.job.set_question(self.question_1)
        self.job.set_state(self.state1)
        self.job.state_mean_by_category()
        dict_to_compare = {"Ohio": {"('Race/Ethnicity', '2 or more races')": 31.6}}
        self.assertEqual(self.job.result, dict_to_compare)

if __name__ == '__main__':
    unittest.main()