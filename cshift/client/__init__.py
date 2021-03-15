from .client_comparison_pipeline import ClientComparisonPipeline as ComparisonPipeline
from .client_config import ClientConfig as Config
from .client_dataset import ClientDataset as Dataset
from .client_model import ClientModel as Model
from .client_result import ClientResult as Result

def configure(username=None, api_key=None):
    config = Config(username=username, api_key=api_key)
    config.write()

def teardown():
    Config.cleanup()