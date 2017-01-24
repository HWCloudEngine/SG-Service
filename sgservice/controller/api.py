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

import six

from oslo_config import cfg
from oslo_log import log as logging

from sgservice.controller.client_factory import ClientFactory
from sgservice.controller import rpcapi as protection_rpcapi
from sgservice.db import base
from sgservice import exception
from sgservice.i18n import _, _LI, _LE
from sgservice import objects
from sgservice.objects import fields
from sgservice import utils

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class API(base.Base):
    """API for interacting with the controller manager."""

    def __init__(self, db_driver=None):
        self.controller_rpcapi = protection_rpcapi.ControllerAPI()
        super(API, self).__init__(db_driver)

    def get(self, context, volume_id):
        try:
            volume = objects.Volume.get_by_id(context, volume_id)
            LOG.info(_LI("Volume info retrieved successfully."),
                     resource=volume)
            return volume
        except Exception:
            raise exception.VolumeNotFound(volume_id)

    def enable_sg(self, context, volume_id, name=None, description=None):
        try:
            self.get(context, volume_id)
            msg = (_LE("The volume '%s' already enabled SG.") % volume_id)
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)
        except exception.VolumeNotFound:
            pass

        cinder_client = ClientFactory.create_client("cinder", context)
        try:
            cinder_volume = cinder_client.volumes.get(volume_id)
        except Exception:
            msg = (_("Get the volume '%s' from cinder failed.") % volume_id)
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)

        if cinder_volume.status != 'available':
            msg = (_("The cinder volume '%(vol_id)s' status to be enabled sg "
                     "must available, but current is %(status)s.") %
                   {'vol_id': volume_id,
                    'status': cinder_volume.status})
            LOG.error(msg)
            raise exception.InvalidVolume(reason=msg)

        if name is None:
            name = cinder_volume.name
        if description is None:
            description = cinder_volume.description

        volume_properties = {
            'user_id': context.user_id,
            'project_id': context.project_id,
            'status': fields.VolumeStatus.ENABLING,
            'display_name': name,
            'display_description': description,
            'availability_zone': cinder_volume.availability_zone
        }

        try:
            objects.Volume.get_by_id(context, id=volume_id,
                                     read_deleted='only')
            volume = objects.Volume.renable(context, volume_id,
                                            volume_properties)
        except Exception:
            volume_properties['id'] = volume_id
            volume = objects.Volume(context=context, **volume_properties)
            volume.create()
        volume.save()

        self.controller_rpcapi.enable_sg(context, volume=volume)
        return volume

    def disable_sg(self, context, volume, cascade=False):
        if volume.status != fields.VolumeStatus.ENABLED:
            msg = (_('Volume %(vol_id)s to be disabled-sg status must be'
                     'enabled, but current status is %(status)s.')
                   % {'vol_id': volume.id,
                      'status': volume.status})
            raise exception.InvalidVolume(reason=msg)

        if volume.replication_id is not None:
            msg = (_("Volume '%(vol_id)s' belongs to the replication "
                     "'%(rep_id)s', can't be disable.")
                   % {'vol_id': volume.id,
                      'rep_id': volume.replication_id})
            raise exception.InvalidVolume(reason=msg)

        snapshots = objects.SnapshotList.get_all_by_volume(context, volume.id)

        if cascade is False:
            if len(snapshots) != 0:
                msg = _(
                    'Unable disable-sg this volume with snapshot or backup.')
                raise exception.InvalidVolume(reason=msg)

        excepted = {'status': ('available', 'error', 'deleting')}
        values = {'status': 'deleting'}
        if cascade:
            for s in snapshots:
                result = s.conditional_update(values, excepted)
                if not result:
                    volume.update({'status': fields.VolumeStatus.ERROR})
                    volume.save()

                    msg = _('Failed to update snapshot.')
                    raise exception.InvalidVolume(reason=msg)

        volume.update({'status': 'disabling'})
        volume.save()
        self.controller_rpcapi.disable_sg(context, volume=volume,
                                          cascade=cascade)
        return volume

    def get_all(self, context, marker=None, limit=None, sort_keys=None,
                sort_dirs=None, filters=None, offset=None):
        if filters is None:
            filters = {}

        all_tenants = utils.get_bool_params('all_tenants', filters)

        try:
            if limit is not None:
                limit = int(limit)
                if limit < 0:
                    msg = _('limit param must be positive')
                    raise exception.InvalidInput(reason=msg)
        except ValueError:
            msg = _('limit param must be an integer')
            raise exception.InvalidInput(reason=msg)

        if filters:
            LOG.debug("Searching by: %s.", six.text_type(filters))

        if context.is_admin and all_tenants:
            # Need to remove all_tenants to pass the filtering below.
            del filters['all_tenants']
            volumes = objects.VolumeList.get_all(context, marker, limit,
                                                 sort_keys=sort_keys,
                                                 sort_dirs=sort_dirs,
                                                 filters=filters,
                                                 offset=offset)
        else:
            volumes = objects.VolumeList.get_all_by_project(
                context, context.project_id, marker, limit,
                sort_keys=sort_keys, sort_dirs=sort_dirs, filters=filters,
                offset=offset)

        LOG.info(_LI("Get all volumes completed successfully."))
        return volumes
