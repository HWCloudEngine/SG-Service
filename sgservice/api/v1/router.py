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
from sgservice.api.v1 import backups
from sgservice.api.v1 import checkpoints
from sgservice.api.v1 import replicates
from sgservice.api.v1 import replications
from sgservice.api.v1 import snapshots
from sgservice.api.v1 import volumes
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

        volumes_resources = volumes.create_resource()
        mapper.resource("volume", "volumes",
                        controller=volumes_resources,
                        collection={'detail': 'GET'},
                        member={'action': 'POST'})

        replicates_resource = replicates.create_resource()
        mapper.resource('replicate', "volume_replicate",
                        controller=replicates_resource,
                        member={'action': 'POST'})

        snapshots_resources = snapshots.create_resource()
        mapper.resource("snapshot", "snapshots",
                        controller=snapshots_resources,
                        collection={'detail': 'GET'},
                        member={'action': 'POST', 'rollback': 'POST'})

        backups_resources = backups.create_resource()
        mapper.resource("backup", "backups",
                        controller=backups_resources,
                        collection={'detail': 'GET'},
                        member={'action': 'POST', 'restore': 'POST',
                                'import_record': 'POST',
                                'export_record': 'POST'})

        checkpoints_resources = checkpoints.create_resource()
        mapper.resource("checkpoint", "checkpoints",
                        controller=checkpoints_resources,
                        collection={'detail': 'GET'},
                        member={'action': 'POST', 'rollback': 'POST',
                                'reset_status': 'POST'})

        super(APIRouter, self).__init__(mapper)
