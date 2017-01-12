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

from cinderclient import client as cc
from oslo_config import cfg
from oslo_log import log as logging

from sgservice.i18n import _LI, _LE
from sgservice.controller.clients import utils

LOG = logging.getLogger(__name__)

cinder_client_opts = [
    cfg.StrOpt('cinder_version',
               default='2',
               help='Version of the cinder client.'),
    cfg.StrOpt('cinder_endpoint',
               help='URL of the cinder endpoint.'),
    cfg.StrOpt('cinder_catalog_info',
               default='volumev2:cinderv2:publicURL',
               help='Info to match when looking for cinder in the service '
                    'catalog. Format is: separated values of the form: '
                    '<service_type>:<service_name>:<endpoint_type> - '
                    'Only used if cinder_endpoint is unset'),
    cfg.StrOpt('cinder_ca_cert_file',
               default=None,
               help='Location of the CA certificate file '
                    'to use for client requests in SSL connections.'),
    cfg.BoolOpt('cinder_auth_insecure',
                default=True,
                help='Bypass verification of server certificate when '
                     'making SSL connection to Cinder.'),
]

CONF = cfg.CONF
CONF.register_opts(cinder_client_opts, group='cinder_client')


def create(context):
    try:
        url = utils.get_url('cinder', context,
                            append_project_fmt='%(url)s/%(project)s')
    except Exception:
        LOG.error(_LE("Get cinder service endpoint url failed."))
        raise

    LOG.info(_LI('Creating cinder client with url %s.'), url)

    args = {
        'project_id': context.project_id,
        'cacert': CONF.cinder_client.cinder_ca_cert_file,
        'insecure': CONF.cinder_client.cinder_auth_insecure,
    }

    client = cc.Client(CONF.cinder_client.cinder_version, **args)
    client.client.auth_token = context.auth_token
    client.client.management_url = url
    return client
