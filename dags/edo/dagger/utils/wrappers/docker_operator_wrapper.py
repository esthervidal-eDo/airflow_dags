from airflow.operators.docker_operator import DockerOperator
from airflow.utils.decorators import apply_defaults


class DockerOperatorWrapper(DockerOperator):
    template_fields = ('volumes', 'image') + DockerOperator.template_fields

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DockerOperatorWrapper, self).__init__(*args, **kwargs)
