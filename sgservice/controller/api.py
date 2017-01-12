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

"""Handles all requests relating to controller service."""

# from cinderclient.exceptions import NotFound
from oslo_config import cfg
from oslo_log import log as logging

# from sgservice.controller.client_factory import ClientFactory
from sgservice.controller import rpcapi as protection_rpcapi
from sgservice.db import base
from sgservice import exception
from sgservice.i18n import _, _LI
from sgservice import objects
from sgservice.objects import fields

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class VolumeAPI(base.Base):
    """API for interacting with the controller manager."""

    def __init__(self, db_driver=None):
        self.controller_rpcapi = protection_rpcapi.ControllerAPI()
        super(VolumeAPI, self).__init__(db_driver)

    def get(self, context, volume_id):
        volume = objects.Volume.get_by_id(context, volume_id)
        LOG.info(_LI("Volume info retrieved successfully."), resource=volume)
        return volume

    def enable_sg(self, context, volume_id):
        volume_properties = {
            'id': volume_id,
            'user_id': context.user_id,
            'project_id': context.project_id,
            'status': fields.VolumeStatus.ENABLING
        }
        volume = objects.Volume(context=context, **volume_properties)
        volume.create()

        self.controller_rpcapi.enable_sg(context, volume=volume)
        return volume

    def disable_sg(self, context, volume, cascade=False):
        if volume.status != 'enabled':
            msg = (_('Volume %(vol_id)s to be disabled-sg status must be'
                     'enabled, but current status is %(status)s.')
                   % {'vol_id': volume.id,
                      'status': volume.status})
            raise exception.InvalidVolume(reason=msg)
        volume.update({'status': 'disabling'})
        volume.save()
        self.controller_rpcapi.disable_sg(context, volume=volume,
                                          cascade=cascade)
        return volume
