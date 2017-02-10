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

"""The checkpoints api."""

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import uuidutils
import webob
from webob import exc

from sgservice.api import common
from sgservice.api.openstack import wsgi
from sgservice.controller.api import API as ServiceAPI
from sgservice import exception
from sgservice.i18n import _, _LI
from sgservice import utils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


class CheckpointViewBuilder(common.ViewBuilder):
    """Model a server API response as a python dictionary."""

    _collection_name = "checkpoints"

    def __init__(self):
        """Initialize view builder."""
        super(CheckpointViewBuilder, self).__init__()

    def detail(self, request, checkpoint):
        """Detailed view of a single checkpoint."""
        checkpoint_ref = {
            'replication': {
                'id': checkpoint.get('id'),
                'status': checkpoint.get('status'),
                'name': checkpoint.get('display_name'),
                'description': checkpoint.get('display_description'),
                'replication_id': checkpoint.get('replication_id')
            }
        }
        return checkpoint_ref

    def detail_list(self, request, checkpoints, checkpoint_count=None):
        """Detailed view of a list of checkpoints."""
        return self._list_view(self.detail, request, checkpoints,
                               checkpoint_count,
                               self._collection_name)

    def _list_view(self, func, request, checkpoints, checkpoint_count,
                   coll_name=_collection_name):
        """Provide a view for a list of checkpoints.

        :param func: Function used to format the checkpoint data
        :param request: API request
        :param checkpoints: List of checkpoints in dictionary format
        :param checkpoint_count: Length of the original list of checkpoints
        :param coll_name: Name of collection, used to generate the next link
                          for a pagination query
        :returns: Checkpoint data in dictionary format
        """
        checkpoints_list = [func(request, checkpoint)['checkpoint'] for
                            checkpoint in checkpoints]
        checkpoints_links = self._get_collection_links(request,
                                                       checkpoints,
                                                       coll_name,
                                                       checkpoint_count)
        checkpoints_dict = {}
        checkpoints_dict['checkpoints'] = checkpoints_list
        if checkpoints_links:
            checkpoints_dict['checkpoints_links'] = checkpoints_links

        return checkpoints_dict


class CheckpointsController(wsgi.Controller):
    """The Checkpoints API controller for the SG-Service."""

    _view_builder_class = CheckpointViewBuilder

    def __init__(self):
        self.service_api = ServiceAPI()
        super(CheckpointsController, self).__init__()

    def show(self, req, id):
        """Return data about the given checkpoints."""
        LOG.info(_LI("Show checkpoint with id: %s"), id)
        if not uuidutils.is_uuid_like(id):
            msg = _("Invalid checkpoint id provided.")
            LOG.error(msg)
            raise exception.InvalidUUID(id)

        context = req.environ['sgservice.context']
        checkpoint = self.service_api.get_checkpoint(context, id)
        return self._view_builder.detail(req, checkpoint)

    def delete(self, req, id):
        """Delete a checkpoint."""
        LOG.info(_LI("Delete checkpoint with id: %s"), id)
        if not uuidutils.is_uuid_like(id):
            msg = _("Invalid checkpoint id provided.")
            LOG.error(msg)
            raise exception.InvalidUUID(id)

        context = req.environ['sgservice.context']
        checkpoint = self.service_api.get_rcheckpoint(context, id)
        self.service_api.delete_replication(context, checkpoint)
        return webob.Response(status_int=202)

    def index(self, req):
        """Returns a list of checkpoints, transformed through view builder."""
        LOG.info(_LI("Show checkpoint list"))
        context = req.environ['sgservice.context']
        params = req.params.copy()
        marker, limit, offset = common.get_pagination_params(params)
        sort_keys, sort_dirs = common.get_sort_params(params)
        filters = params

        utils.remove_invaild_filter_options(
            context, filters, self._get_replication_filter_options())
        utils.check_filters(filters)

        checkpoints = self.service_api.get_all_checkpoints(
            context, marker=marker, limit=limit, sort_keys=sort_keys,
            sort_dirs=sort_dirs, filters=filters, offset=offset)

        retval_checkpoints = self._view_builder.detail_list(req, checkpoints)
        LOG.info(_LI("Show checkpoint list request issued successfully."))
        return retval_checkpoints

    def create(self, req, body):
        """Creates a new checkpoint."""
        if not self.is_valid_body(body, 'checkpoint'):
            raise exc.HTTPUnprocessableEntity()
        LOG.debug('Create replication request body: %s', body)
        context = req.environ['sgservice.context']
        checkpoint = body['checkpoint']

        replication_id = checkpoint.get('replication_id', None)
        if replication_id is None:
            msg = _('Incorrect request body format')
            raise webob.exc.HTTPBadRequest(explanation=msg)

        name = checkpoint.get('name', 'checkpoint-%s' % replication_id)
        description = checkpoint.get('description', name)

        if not uuidutils.is_uuid_like(replication_id):
            msg = _("Invalid replication id provided.")
            LOG.error(msg)
            raise exception.InvalidUUID(replication_id)

        replication = self.service_api.get_replication(context,
                                                       replication_id)
        checkpoint = self.service_api.create_checkpoint(context, replication)
        return self._view_builder.detail(req, checkpoint)

    def update(self, req, id, body):
        """Update a checkpoint."""
        LOG.info(_LI("Update checkpoint with id: %s"), id)
        if not uuidutils.is_uuid_like(id):
            msg = _("Invalid checkpoint id provided.")
            LOG.error(msg)
            raise exception.InvalidUUID(id)

        context = req.environ['sgservice.context']
        checkpoint = self.service_api.get_checkpoint(context, id)

        # TODO(luobin): implement update checkpoint [name, description]
        return self._view_builder.detail(req, checkpoint)


def create_resource():
    return wsgi.Resource(CheckpointsController())
