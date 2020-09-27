import sys
import os

root_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '', 'lambdas')

sys.path.append(os.path.join(root_dir, '', 'python_etl'))
sys.path.append(os.path.join(root_dir, '', 'covid_api'))
sys.path.append(os.path.join(root_dir, '', 'sns_lambda'))
