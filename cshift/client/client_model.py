from cshift.proto import cshift_pb2 as pb2

class ClientModel:
    def __init__(self,
                 name: str = None,
                 training_set_spec: pb2.DatasetSpec = None):
        self.model_spec = pb2.ModelSpec(
            name=name,
            training_set_spec=training_set_spec
        )

