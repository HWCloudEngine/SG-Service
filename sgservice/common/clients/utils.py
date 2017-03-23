#    Copyright (c) 2014 Huawei Technologies Co., Ltd.
#
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

import keystoneauth1.identity as keystone_auth
from keystoneauth1 import session as keystone_auth_session
from keystoneclient.v2_0 import client as kc

from oslo_log import log as logging

LOG = logging.getLogger(__name__)


def get_admin_session(auth_url, username, password,
                      project_name=None, project_domain_name=None,
                      user_domain_name=None, verify=False, timeout=180):
    if 'v3' in auth_url:
        auth = keystone_auth.V3Password(
            auth_url=auth_url,
            username=username,
            password=password,
            project_name=project_name,
            project_domain_name=project_domain_name,
            user_domain_name=user_domain_name)
    else:
        auth = keystone_auth.V2Password(
            auth_url,
            username=username,
            password=password,
            tenant_name=project_name)

    session = keystone_auth_session.Session(
        auth=auth,
        timeout=timeout,
        verify=verify)
    return session


def get_management_url(auth_url, tenant_name, username, password,
                       region_name, service_type, insecure=False,
                       cacert=None):
    kwargs = {
        'auth_url': auth_url,
        'tenant_name': tenant_name,
        'username': username,
        'password': password,
        'insecure': insecure,
        'cacert': cacert
    }
    keystoneclient = kc.Client(**kwargs)
    url = _get_url(keystoneclient, service_type=service_type,
                   attr='region', endpoint_type='publicURL',
                   filter_value=region_name)

    management_url = url.rpartition("/")[0]
    return management_url


def _get_url(kc, **kwargs):
    return kc.service_catalog.url_for(**kwargs)
