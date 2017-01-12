# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Controller Service
"""

from oslo_config import cfg
from oslo_log import log as logging
import oslo_messaging as messaging

from sgservice.i18n import _LI
from sgservice import manager
from sgservice import objects

LOG = logging.getLogger(__name__)

CONF = cfg.CONF


class ControllerManager(manager.Manager):
    """SGService Controller Manager."""

    RPC_API_VERSION = '1.0'

    target = messaging.Target(version=RPC_API_VERSION)

    def __init__(self, service_name=None, *args, **kwargs):
        super(ControllerManager, self).__init__(*args, **kwargs)

    def init_host(self, **kwargs):
        """Handle initialization if this is a standalone service"""
        LOG.info(_LI("Starting controller service"))

    def enable_sg(self, context, volume_id):
        volume = objects.Volume.get_by_id(context, volume_id)
        volume.update({'replication_zone': CONF.replication_zone})
        volume.save()
        LOG.info(_LI("Enable-SG for this volume with id %s"), volume_id)
        pass

    def disable_sg(self, context, volume_id, cascaded=False):
        objects.Volume.get_by_id(context, volume_id)
        LOG.info(_LI("Disable-SG for this volume with id %s"), volume_id)
        pass
