from tortoise import fields
from tortoise.models import Model

class Job(Model):
    """
    Job model to track video processing tasks
    """
    id = fields.IntField(pk=True)
    job_id = fields.CharField(max_length=255, unique=True, index=True)
    video_path = fields.CharField(max_length=512)
    lut_path = fields.CharField(max_length=512)
    num_mpi_processes = fields.IntField(default=4)
    enable_intra_node_parallelism = fields.BooleanField(default=True)
    status = fields.CharField(max_length=50, default="pending")  # pending, processing, completed, failed
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)
    error_message = fields.TextField(null=True)

    class Meta:
        table = "jobs"

    def __str__(self):
        return f"Job {self.job_id} - {self.status}"