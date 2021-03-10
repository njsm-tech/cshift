from cshift.proto import cshift_pb2 as pb2

class GcsPath:
    def __init__(self, bucket, path_ext):
        self.bucket = bucket
        self.path_ext = path_ext

    @classmethod
    def from_message(cls, msg: pb2.DatasetSpec.GcsPath):
        return cls(bucket=msg.bucket, path_ext=msg.path_ext)

    @property
    def path(self) -> str:
        return "gs://{bucket}/{path_ext}".format(
            bucket=self.bucket, path_ext=self.path_ext)