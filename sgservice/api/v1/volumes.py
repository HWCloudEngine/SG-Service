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

"""The replications api."""
import webob

from oslo_config import cfg
from oslo_log import log as logging

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice.i18n import _, _LI
from sgservice.objects import fields
from sgservice import utils

query_volume_filters_opts = cfg.ListOpt(
    'query_volume_filters',
    default=['name', 'status'],
    help='Volume filter options which non-admin user could use to query '
         'volumes.')

CONF = cfg.CONF
CONF.register_opt(query_volume_filters_opts)

LOG = logging.getLogger(__name__)


class VolumeViewBuilder(common.ViewBuilder):
    """Model a server API response as a python dictionary."""

    _collection_name = "volumes"

    def __init__(self):
        """Initialize view builder."""
        super(VolumeViewBuilder, self).__init__()

    def _get_attachments(self, volume):
        attachments = []
        for attachment in volume.volume_attachment:
            attachments.append({'attachment_id': attachment.id,
                                'server_id': attachment.instance_uuid,
                                'device': attachment.mountpoint})
        return attachments

    def attach_summary(self, request, attach):
        """Generic, non-detailed view of restore"""
        return {
            'attach': {
                'instance_id': attach['instance_uuid'],
                'volume_id': attach['volume_id'],
                'device': attach['mountpoint']
            }
        }

    def detail(self, request, volume):
        """Detailed view of a single volume."""
        volume_ref = {
            'volume': {
                'id': volume.get('id'),
                'status': volume.get('status'),
                'name': volume.get('display_name'),
                'description': volume.get('display_description'),
                'size': volume.get('size'),
                'availability_zone': volume.get('availability_zone'),
                'replication_zone': volume.get('replication_zone'),
                'replication_id': volume.get('replication_id'),
                'replicate_status': volume.get('replicate_status'),
                'replicate_mode': volume.get('replicate_mode'),
                'peer_volume': volume.get('peer_volume'),
                'access_mode': volume.get('access_mode'),
                'attachments': self._get_attachments(volume)
            }
        }
        return volume_ref

    def detail_list(self, request, volumes, volume_count=None):
        """Detailed view of a list of volumes."""
        return self._list_view(self.detail, request, volumes,
                               volume_count,
                               self._collection_name)

    def _list_view(self, func, request, volumes, volume_count,
                   coll_name=_collection_name):
        """Provide a view for a list of volumes.

        :param func: Function used to format the volume data
        :param request: API request
        :param volumes: List of volumes in dictionary format
        :param volume_count: Length of the original list of volumes
        :param coll_name: Name of collection, used to generate the next link
                          for a pagination query
        :returns: Volume data in dictionary format
        """
        volumes_list = [func(request, volume)['volume'] for
                        volume in volumes]
        volumes_links = self._get_collection_links(request,
                                                   volumes,
                                                   coll_name,
                                                   volume_count)
        volumes_dict = {}
        volumes_dict['volumes'] = volumes_list
        if volumes_links:
            volumes_dict['volumes_links'] = volumes_links

        return volumes_dict


class VolumesController(wsgi.Controller):
    """The Volumes API controller for the SG-Service."""

    _view_builder_class = VolumeViewBuilder

    def __init__(self):
        self.service_api = ServiceAPI()
        super(VolumesController, self).__init__()

    def _get_volume_filter_options(self):
        return CONF.query_volume_filters

    def show(self, req, id):
        """Return data about the given volumes."""
        LOG.info(_LI("Show volume with id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        return self._view_builder.detail(req, volume)

    def delete(self, req, id):
        """Delete a sg volume."""
        LOG.info(_LI("Delete sg volume, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        self.service_api.delete(context, volume)
        return webob.Response(status_int=202)

    def index(self, req):
        """Returns a list of volumes, transformed through view builder."""
        LOG.info(_LI("Show volume list"))
        context = req.environ['sgservice.context']
        params = req.params.copy()
        marker, limit, offset = common.get_pagination_params(params)
        sort_keys, sort_dirs = common.get_sort_params(params)
        filters = params

        utils.remove_invaild_filter_options(
            context, filters, self._get_volume_filter_options())
        utils.check_filters(filters)

        if 'name' in sort_keys:
            sort_keys[sort_keys.index('name')] = 'display_name'

        if 'name' in filters:
            filters['display_name'] = filters.pop('name')

        volumes = self.service_api.get_all(
            context, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

        retval_volumes = self._view_builder.detail_list(req, volumes)
        LOG.info(_LI("Show volume list request issued successfully."))
        return retval_volumes

    def create(self, req, body):
        """Creates a new volume from snapshot or checkpoint."""
        LOG.debug('Create volume from snapshot, request body: %s', body)
        context = req.environ['sgservice.context']
        volume = body['volume']

        volume_type = volume.get('volume_type', None)
        availability_zone = volume.get('availability_zone', None)
        volume_id = volume.get('volume_id', None)
        size = volume.get('size', None)

        # create from snapshot
        snapshot_id = volume.get('snapshot_id')
        if snapshot_id is not None:
            name = volume.get('name', 'volume-%s' % snapshot_id)
            description = volume.get('description', name)
            snapshot = self.service_api.get_snapshot(context, snapshot_id)
            volume = self.service_api.create_volume(
                context, snapshot=snapshot,
                volume_type=volume_type,
                availability_zone=availability_zone,
                description=description,
                name=name, volume_id=volume_id,
                size=size)
            return self._view_builder.detail(req, volume)

        # create from checkpoint
        checkpoint_id = volume.get('checkpoint_id')
        if checkpoint_id is not None:
            name = volume.get('name', 'volume-%s' % checkpoint_id)
            description = volume.get('description', name)
            checkpoint = self.service_api.get_checkpoint(context,
                                                         checkpoint_id)
            volume = self.service_api.create_volume(
                context, checkpoint=checkpoint,
                volume_type=volume_type,
                availability_zone=availability_zone,
                description=description,
                name=name, volume_id=volume_id)
            return self._view_builder.detail(req, volume)

        msg = _('Incorrect request body format, create volume must specified '
                'a snapshot or checkpoint')
        raise webob.exc.HTTPBadRequest(explanation=msg)

    def update(self, req, id, body):
        """Update a volume."""
        LOG.info(_LI("Update snapshot with id: %s"), id)
        context = req.environ['sgservice.context']
        if not body:
            msg = _("Missing request body")
            raise webob.exc.HTTPBadRequest(explanation=msg)
        if 'volume' not in body:
            msg = (_("Missing required element '%s' in request body"),
                   'volume')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        volume = body['volume']
        update_dict = {}

        valid_update_keys = (
            'name',
            'description',
            'display_name',
            'display_description',
        )
        for key in valid_update_keys:
            if key in volume:
                update_dict[key] = volume[key]
        self.validate_name_and_description(update_dict)
        if 'name' in update_dict:
            update_dict['display_name'] = update_dict.pop('name')
        if 'description' in update_dict:
            update_dict['display_description'] = update_dict.pop('description')

        volume = self.service_api.get(context, id)
        volume.update(update_dict)
        volume.save()
        return self._view_builder.detail(req, volume)

    @wsgi.action('enable')
    def enable(self, req, id, body):
        """Enable-SG a available volume."""
        LOG.debug('Enable volume SG, volume_id: %s', id)
        context = req.environ['sgservice.context']
        params = body['enable']
        params = {} if params is None else params

        name = params.get('name', None)
        description = params.get('description', None)
        metadata = params.get('metadata', None)
        volume = self.service_api.enable_sg(context, id, name=name,
                                            description=description,
                                            metadata=metadata)
        return self._view_builder.detail(req, volume)

    @wsgi.action('disable')
    def disable(self, req, id, body):
        """Disable a enabled-volume."""
        LOG.info(_LI("Disable volume SG, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        volume = self.service_api.disable_sg(context, volume)
        return self._view_builder.detail(req, volume)

    @wsgi.action('attach')
    def attach(self, req, id, body):
        """Add sg-volume attachment metadata."""
        LOG.info(_LI("Add SG-volume attachment, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)

        params = body['attach']
        params = {} if params is None else params
        instance_uuid = params.get('instance_uuid', None)

        mode = params.get('mode', None)
        if mode is None:
            mode = 'rw'
        if instance_uuid is None:
            msg = _("Invalid request to attach volume to an invalid target")
            raise webob.exc.HTTPBadRequest(explanation=msg)
        if mode not in ('rw', 'ro'):
            msg = _("Invalid request to attach volume to an invalid mode. "
                    "Attaching mode should be 'rw' or 'ro'.")
            raise webob.exc.HTTPBadRequest(explanation=msg)

        attach_result = self.service_api.attach(context, volume, instance_uuid,
                                                mode)
        return self._view_builder.attach_summary(req, attach_result)

    @wsgi.action('detach')
    def detach(self, req, id, body):
        """Clear attachment metadata."""
        LOG.info(_LI("Clear SG-volume attachment with volume_id: %s"),
                 id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        params = body['detach']
        params = {} if params is None else params
        instance_uuid = params.get('instance_uuid', None)
        self.service_api.detach(context, volume, instance_uuid)
        return webob.Response(status_int=202)

    @wsgi.action('reset_status')
    def reset_status(self, req, id, body):
        """reset volume status"""
        LOG.info(_LI("Reset volume status, id: %s"), id)
        status = body['reset_status'].get('status',
                                          fields.VolumeStatus.ENABLED)
        if status not in fields.VolumeStatus.ALL:
            msg = _("Invalid status provided.")
            LOG.error(msg)
            raise exception.InvalidStatus(status=status)

        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        volume.status = status
        volume.save()
        return webob.Response(status_int=202)


def create_resource():
    return wsgi.Resource(VolumesController())
