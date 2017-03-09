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
from oslo_utils import uuidutils

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice.i18n import _, _LI
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

    def detail(self, request, volume):
        """Detailed view of a single volume."""
        volume_ref = {
            'volume': {
                'id': volume.get('id'),
                'status': volume.get('status'),
                'availability_zone': volume.get('availability_zone'),
                'replication_zone': volume.get('replication_zone'),
                'replication_id': volume.get('replication_id'),
                'replicate_status': volume.get('replicate_status'),
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

        volumes = self.service_api.get_all(
            context, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

        retval_volumes = self._view_builder.detail_list(req, volumes)
        LOG.info(_LI("Show volume list request issued successfully."))
        return retval_volumes

    @wsgi.action('enable')
    def enable(self, req, id, body):
        """Enable-SG a available volume."""
        LOG.debug('Enable volume SG, volume_id: %s', id)
        context = req.environ['sgservice.context']
        params = body['enable']
        params = {} if params is None else params

        name = params.get('name', None)
        description = params.get('description', None)
        volume = self.service_api.enable_sg(context, id, name=name,
                                            description=description)
        return self._view_builder.detail(req, volume)

    @wsgi.action('disable')
    def disable(self, req, id, body):
        """Disable a enabled-volume."""
        LOG.info(_LI("Disable volume SG, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        volume = self.service_api.disable_sg(context, volume)
        return self._view_builder.detail(req, volume)

    @wsgi.action('reserve')
    def reserve(self, req, id, body):
        """Mark SG-volume as reserved before attach."""
        LOG.info(_LI("Mark SG-volume as reserved, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        self.service_api.reserve_volume(context, volume)
        return webob.Response(status_int=202)

    @wsgi.action('unreserve')
    def unreserve(self, req, id, body):
        """Unmark volume as reserved."""
        LOG.info(_LI("Unmark volume as reserved, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        self.service_api.unreserve_volume(context, volume)
        return webob.Response(status_int=202)

    @wsgi.action('initialize_connection')
    def initialize_connection(self, req, id, body):
        """Initialize volume attachment."""
        LOG.info(_LI("Initialize volume attachment, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)

        params = body['initialize_connection']
        params = {} if params is None else params
        connector = params.get('connector', None)

        info = self.service_api.initialize_connection(
            context, volume, connector)
        return {"connection_info": info}

    @wsgi.action('attach')
    def attach(self, req, id, body):
        """Add sg-volume attachment metadata."""
        LOG.info(_LI("Add SG-volume attachment, volume_id: %s"), id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)

        params = body['attach']
        params = {} if params is None else params
        instance_uuid = params.get('instance_uuid', None)
        host_name = params.get('host_name', None)
        mountpoint = params.get('mountpoint', None)

        mode = params.get('mode', None)
        if mode is None:
            mode = 'rw'
        if instance_uuid is None and host_name is None:
            msg = _("Invalid request to attach volume to an invalid target")
            raise webob.exc.HTTPBadRequest(explanation=msg)
        if mode not in ('rw', 'ro'):
            msg = _("Invalid request to attach volume to an invalid mode. "
                    "Attaching mode should be 'rw' or 'ro'.")
            raise webob.exc.HTTPBadRequest(explanation=msg)

        self.service_api.attach(context, volume, instance_uuid, host_name,
                                mountpoint, mode)
        return webob.Response(status_int=202)

    @wsgi.action('begin_detaching')
    def begin_detaching(self, req, id, body):
        """Update volume status to 'detaching'."""
        LOG.info(_LI("Update volume status to detaching, volume_id: %s"),
                 id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        self.service_api.begin_detaching(context, volume)
        return webob.Response(status_int=202)

    @wsgi.action('roll_detaching')
    def roll_detaching(self, req, id, body):
        """Roll back volume status to 'in-use'."""
        LOG.info(_LI("Roll back volume status to in-use, volume_id: %s"),
                 id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        self.service_api.roll_detaching(context, volume)
        return webob.Response(status_int=202)

    @wsgi.action('detach')
    def detach(self, req, id, body):
        """Clear attachment metadata."""
        LOG.info(_LI("Clear SG-volume attachment with volume_id: %s"),
                 id)
        context = req.environ['sgservice.context']
        volume = self.service_api.get(context, id)
        params = body['detach']
        params = {} if params is None else params
        attachment_id = params.get('attachment_id', None)
        self.service_api.detach(context, volume, attachment_id)
        return webob.Response(status_int=202)

    def create(self, req, body):
        """Creates a new volume from snapshot or checkpoint."""
        LOG.debug('Create volume from snapshot, request body: %s', body)
        context = req.environ['sgservice.context']
        volume = body['volume']

        volume_type = volume.get('volume_type', None)
        availability_zone = volume.get('availability_zone', None)

        # create from snapshot
        snapshot_id = volume.get('snapshot_id')
        if snapshot_id is not None:
            name = volume.get('name', 'volume-%s' % snapshot_id)
            description = volume.get('description', name)
            snapshot = self.service_api.get_snapshot(context, snapshot_id)
            self.service_api.create_volume(context, snapshot=snapshot,
                                           volume_type=volume_type,
                                           availability_zone=availability_zone,
                                           description=description,
                                           name=name)
            return webob.Response(status_int=202)

        # create from checkpoint
        checkpoint_id = volume.get('checkpoint_id')
        if checkpoint_id is not None:
            name = volume.get('name', 'volume-%s' % checkpoint_id)
            description = volume.get('description', name)
            checkpoint = self.service_api.get_checkpoint(context,
                                                         checkpoint_id)
            self.service_api.create_volume(context, checkpoint=checkpoint,
                                           volume_type=volume_type,
                                           availability_zone=availability_zone,
                                           description=description,
                                           name=name)
            return webob.Response(status_int=202)

        msg = _('Incorrect request body format, create volume must specified '
                'a snapshot or checkpoint')
        raise webob.exc.HTTPBadRequest(explanation=msg)


def create_resource():
    return wsgi.Resource(VolumesController())
