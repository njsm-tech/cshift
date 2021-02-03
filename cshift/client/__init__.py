from .client_comparison import ClientComparison as Comparison
from .client_comparison_pipeline import ClientComparisonPipeline as ComparisonPipeline
from .client_comparison_set import ClientComparisonSet as ComparisonSet
from .client_config import ClientConfig as Config
from .client_dataset import ClientDataset as Dataset
from .client_model import ClientModel as Model
from .client_result import ClientResult as Result
from .client_result_set import ClientResultSet as ResultSet

def configure(username=None, api_key=None):
    config = Config(username=username, api_key=api_key)
    config.write()

def teardown():
    Config.cleanup()