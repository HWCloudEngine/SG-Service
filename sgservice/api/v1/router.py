#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from sgservice.api.openstack import ProjectMapper
from sgservice.api.v1 import replications
from sgservice.wsgi import common as wsgi_common


class APIRouter(wsgi_common.Router):
    @classmethod
    def factory(cls, global_conf, **local_conf):
        return cls(ProjectMapper())

    def __init__(self, mapper):
        replications_resources = replications.create_resource()
        mapper.resource("replication", "replications",
                        controller=replications_resources,
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})
        mapper.connect("replication",
                       "/{project_id}/replications/{replication_id}/enable",
                       controller=replications_resources,
                       action='enable',
                       conditions={"method": ['POST']})
        mapper.connect("replication",
                       "/{project_id}/replications/{replication_id}/disable",
                       controller=replications_resources,
                       action='disable',
                       conditions={"method": ['POST']})
        mapper.connect("replication",
                       "/{project_id}/replications/{replication_id}/failover",
                       controller=replications_resources,
                       action='failover',
                       conditions={"method": ['POST']})
        mapper.connect("replication",
                       "/{project_id}/replications/{replication_id}/reverse",
                       controller=replications_resources,
                       action='reverse',
                       conditions={"method": ['POST']})
        super(APIRouter, self).__init__(mapper)
