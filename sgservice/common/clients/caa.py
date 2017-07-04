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

from keystoneclient import exceptions as keystone_exception
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from caaclient import client as cc
from caaclient import exceptions as caa_exception

from sgservice.common.clients import utils
from sgservice.i18n import _LE

LOG = logging.getLogger(__name__)

caa_client_opts = [
    cfg.IntOpt('caa_version',
               default=1,
               help='The version of caa client'),
    cfg.IntOpt('connect_retries',
               default=3),
    cfg.StrOpt('service_type',
               default='caa',
               help='The service type of caa'),
    cfg.StrOpt('region_name',
               default='RegionOne',
               help='Region name of this node'),
    cfg.StrOpt('keystone_auth_url',
               default='http://9.38.55.52:35357/v2.0',
               help='value of keystone url'),
    cfg.StrOpt('caa_admin_username',
               default='caa_admin_username',
               help='username for connecting to caa in admin context'),
    cfg.StrOpt('caa_admin_password',
               default='caa_admin_password',
               help='password for connecting to caa in admin context',
               secret=True),
    cfg.StrOpt('caa_admin_tenant_name',
               default='caa_admin_tenant_name',
               help='tenant name for connecting to caa in admin context'),
    cfg.StrOpt('caa_admin_tenant_domain',
               default='default',
               help='admin tenant domain name'),
    cfg.StrOpt('caa_ca_cert_file',
               default=None,
               help='Location of the CA certificate file '
                    'to use for client requests in SSL connections.'),
    cfg.BoolOpt('caa_auth_insecure',
                default=True,
                help='Bypass verification of server certificate when '
                     'making SSL connection to CAA.'),
    cfg.IntOpt("timeout",
               default=180,
               help="A timeout to pass to requests"),
]

CONF = cfg.CONF
CONF.register_opts(caa_client_opts, 'caa_client')


def get_admin_client():
    if CONF.caa_client.caa_ca_cert_file:
        verify = CONF.caa_client.caa_ca_cert_file
    else:
        verify = False

    try:
        session = utils.get_admin_session(
            auth_url=CONF.caa_client.keystone_auth_url,
            username=CONF.caa_client.caa_admin_username,
            password=CONF.caa_client.caa_admin_password,
            project_name=CONF.caa_client.caa_admin_tenant_name,
            project_domain_name=CONF.caa_client.caa_admin_tenant_domain,
            user_domain_name=CONF.caa_client.caa_admin_tenant_domain,
            verify=verify,
            timeout=CONF.caa_client.timeout)

        client = cc.Client(session=session,
                           version=CONF.caa_client.caa_version,
                           connect_retries=3,
                           region_name=CONF.caa_client.region_name)
        return client
    except keystone_exception.Unauthorized:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Token unauthorized failed for keystoneclient '
                          'constructed when get admin client'))
    except caa_exception.Unauthorized:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Token unauthorized failed for caaClient '
                          'constructed'))
    except Exception:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Failed to get caa python client.'))


def get_project_context_client(context):
    try:
        url = utils.get_management_url(
            auth_url=CONF.caa_client.keystone_auth_url,
            tenant_name=CONF.caa_client.caa_admin_tenant_name,
            username=CONF.caa_client.caa_admin_username,
            password=CONF.caa_client.caa_admin_password,
            region_name=CONF.caa_client.region_name,
            service_type=CONF.caa_client.service_type,
            insecure=CONF.caa_client.caa_auth_insecure,
            cacert=CONF.caa_client.caa_ca_cert_file)
        management_url = url + '/' + context.project_id
        args = {
            'username': context.user_id,
            'auth_url': CONF.caa_client.keystone_auth_url,
            'service_type': CONF.caa_client.service_type,
            'region_name': CONF.caa_client.region_name,
            'insecure': CONF.caa_client.caa_auth_insecure,
            'cacert': CONF.caa_client.caa_ca_cert_file,
            'timeout': CONF.caa_client.timeout,
            'token': context.auth_token
        }
        client = cc.Client(CONF.caa_client.caa_version, management_url,
                           **args)
        return client
    except keystone_exception.Unauthorized:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Token unauthorized failed for keystoneclient '
                          'constructed when get admin caaClient'))
    except caa_exception.Unauthorized:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Token unauthorized failed for caaClient '
                          'constructed'))
    except Exception:
        with excutils.save_and_reraise_exception():
            LOG.error(_LE('Failed to get caa python client.'))
