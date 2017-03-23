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

from novaclient import client as nc
from novaclient import exceptions as nova_exception
from keystoneclient import exceptions as keystone_exception
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils

from sgservice.common.clients import utils
from sgservice.i18n import _LI, _LE

LOG = logging.getLogger(__name__)

nova_client_opts = [
    cfg.IntOpt('nova_version',
               default=2,
               help='The version of nova client'),
    cfg.IntOpt('connect_retries',
               default=3),
    cfg.StrOpt('service_type',
               default='compute',
               help='The service type of nova'),
    cfg.StrOpt('region_name',
               default='RegionOne',
               help='Region name of this node'),
    cfg.StrOpt('keystone_auth_url',
               default='http://9.38.55.52:35357/v2.0',
               help='value of keystone url'),
    cfg.StrOpt('nova_admin_username',
               default='nova_admin_username',
               help='username for connecting to nova in admin context'),
    cfg.StrOpt('nova_admin_password',
               default='nova_admin_password',
               help='password for connecting to nova in admin context',
               secret=True),
    cfg.StrOpt('nova_admin_tenant_name',
               default='nova_admin_tenant_name',
               help='tenant name for connecting to nova in admin context'),
    cfg.StrOpt('nova_admin_tenant_domain',
               default='default',
               help='Admin tenant domain name'),
    cfg.StrOpt('nova_ca_cert_file',
               default=None,
               help='Location of the CA certificate file '
                    'to use for client requests in SSL connections.'),
    cfg.BoolOpt('nova_auth_insecure',
                default=True,
                help='Bypass verification of server certificate when '
                     'making SSL connection to Nova.'),
    cfg.IntOpt("timeout",
               default=180,
               help="A timeout to pass to requests"),
]

CONF = cfg.CONF
CONF.register_opts(nova_client_opts, 'nova_client')


def get_admin_client():
    if CONF.nova_client.nova_ca_cert_file:
        verify = CONF.nova_client.nova_ca_cert_file
    else:
        verify = False

    try:
        session = utils.get_admin_session(
            auth_url=CONF.nova_client.keystone_auth_url,
            username=CONF.nova_client.nova_admin_username,
            password=CONF.nova_client.nova_admin_password,
            project_name=CONF.nova_client.nova_admin_tenant_name,
            project_domain_name=CONF.nova_client.nova_admin_tenant_domain,
            user_domain_name=CONF.nova_client.nova_admin_tenant_domain,
            verify=verify,
            timeout=CONF.nova_client.timeout)

        client = nc.Client(session=session,
                           version=CONF.nova_client.nova_version,
                           connect_retries=3,
                           region_name=CONF.nova_client.region_name)
        return client
    except keystone_exception.Unauthorized:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Token unauthorized failed for keystoneclient '
                          'constructed when get admin client'))
    except nova_exception.Unauthorized:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Token unauthorized failed for novaClient '
                          'constructed'))
    except Exception:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Failed to get nova python client.'))


def get_project_context_client(context):
    try:
        url = utils.get_management_url(
            auth_url=CONF.nova_client.keystone_auth_url,
            tenant_name=CONF.nova_client.nova_admin_tenant_name,
            username=CONF.nova_client.nova_admin_username,
            password=CONF.nova_client.nova_admin_password,
            region_name=CONF.nova_client.region_name,
            service_type=CONF.nova_client.service_type,
            insecure=CONF.nova_client.nova_auth_insecure,
            cacert=CONF.nova_client.nova_ca_cert_file)
        management_url = url + '/' + context.project_id
        args = {
            'project_id': context.project_id,
            'auth_url': CONF.nova_client.keystone_auth_url,
            'service_type': CONF.nova_client.service_type,
            'region_name': CONF.nova_client.region_name,
            'username': context.user_id,
            'insecure': CONF.nova_client.nova_auth_insecure,
            'cacert': CONF.nova_client.nova_ca_cert_file,
            'timeout': CONF.nova_client.timeout
        }
        client = nc.Client(CONF.nova_client.nova_version, **args)
        client.client.auth_token = context.auth_token
        client.client.management_url = management_url
        return client
    except keystone_exception.Unauthorized:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Token unauthorized failed for keystoneclient '
                          'constructed when get admin novaClient'))
    except nova_exception.Unauthorized:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Token unauthorized failed for novaClient '
                          'constructed'))
    except Exception:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Failed to get nova python client.'))
