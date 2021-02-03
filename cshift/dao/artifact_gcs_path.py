from cshift.proto import cshift_pb2 as pb2

class ArtifactGcsPath:
    def __init__(self,
                 bucket: str = None,
                 username: str = None,
                 project: str = None,
                 artifact_type: pb2.ArtifactType = None,
                 artifact_name: str = None,
                 artifact_version: str = None):
        self.bucket = bucket
        self.username = username
        self.project = project
        self.artifact_type = artifact_type
        self.artifact_name = artifact_name
        self.artifact_version = artifact_version

    @classmethod
    def from_message(cls, msg: pb2.ArtifactGcsPath):
        return cls(
            bucket=msg.bucket,
            username=msg.username,
            project=msg.project,
            artifact_type=msg.artifact_type,
            artifact_name=msg.artifact_name,
            artifact_version=msg.artifact_version)

    def to_message(self):
        return pb2.ArtifactGcsPath(
            bucket=self.bucket,
            username=self.username,
            project=self.project,
            artifact_type=self.artifact_type,
            artifact_name=self.artifact_name,
            artifact_version=self.artifact_version)

    @property
    def path_ext(self) -> str:
        # TODO: include versioning of artifacts in path
        return "{username}/{project}/{artifact_type}/{artifact_name}".format(
            username=self.username,
            project=self.project,
            artifact_type=str(self.artifact_type),
            artifact_name=self.artifact_name)

    @property
    def path(self) -> str:
        return "gs://{bucket}/{path_ext}".format(
            bucket=self.bucket,
            path_ext=self.path_ext)