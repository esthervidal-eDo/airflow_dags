import json

from airflow.contrib.operators.gcp_container_operator import GKEPodOperator
from airflow.utils.decorators import apply_defaults


class GKEPodOperatorWrapper(GKEPodOperator):
    template_fields = ('node_pool',) + GKEPodOperator.template_fields

    @apply_defaults
    def __init__(self, node_pool, *args, **kwargs):
        super(GKEPodOperatorWrapper, self).__init__(*args, **kwargs)
        self.node_pool = node_pool

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise TypeError('Wrong type: {}'.format(type(key).__name__))

    def __get_node_affinity__(self):
        return {
            'nodeAffinity': {
                'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{
                        'matchExpressions': [{
                            'key': 'cloud.google.com/gke-nodepool',
                            'operator': 'In',
                            'values': [
                                self.node_pool
                            ]
                        }]
                    }]
                }
            }
        }

    def execute(self, context):
        self.affinity = self.__get_node_affinity__()
        super(GKEPodOperatorWrapper, self).execute(context)
