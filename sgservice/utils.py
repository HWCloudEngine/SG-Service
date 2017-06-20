# Licensed under the Apache License, Version 2.0 (the "License"); you may
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

"""Utilities and helper functions."""
import ast
import os
import re
import six

from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import strutils
from oslo_utils import timeutils

from sgservice import exception
from sgservice.i18n import _

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def find_config(config_path):
    """Find a configuration file using the given hint.

    :param config_path: Full or relative path to the config.
    :returns: Full path of the config, if it exists.
    :raises: `sgservice.exception.ConfigNotFound`

    """
    possible_locations = [
        config_path,
        os.path.join("/var/lib/sgservice", "etc", "sgservice", config_path),
        os.path.join("/var/lib/sgservice", "etc", config_path),
        os.path.join("/var/lib/sgservice", config_path),
        "/etc/sgservice/%s" % config_path,
    ]

    for path in possible_locations:
        if os.path.exists(path):
            return os.path.abspath(path)

    raise exception.ConfigNotFound(path=os.path.abspath(config_path))


def service_is_up(service):
    """Check whether a service is up based on last heartbeat."""
    last_heartbeat = service['updated_at'] or service['created_at']
    # Timestamps in DB are UTC.
    elapsed = (timeutils.utcnow(with_timezone=True) -
               last_heartbeat).total_seconds()
    return abs(elapsed) <= CONF.service_down_time


def remove_invaild_filter_options(context, filters, allowed_search_options):
    """Remove search options that are not valid for non-admin API/context"""

    if context.is_admin:
        return

    unknown_options = [opt for opt in filters
                       if opt not in allowed_search_options]
    bad_options = ','.join(unknown_options)
    LOG.debug("Removing options '%s' from query.", bad_options)
    for opt in unknown_options:
        del filters[opt]


def check_filters(filters):
    for k, v in six.iteritems(filters):
        try:
            filters[k] = ast.literal_eval(v)
        except (ValueError, SyntaxError):
            LOG.debug('Could not evaluate value %s, assuming string', v)


def is_valid_boolstr(val):
    val = str(val).lower()
    return val in ('true', 'false', 'yes', 'no', 'y', 'n', '1', '0')


def get_bool_params(param_string, params):
    param = params.get(param_string, False)
    if not is_valid_boolstr(param):
        msg = _('Value %(param)s for %(param_string)s is not a boolean') % {
            'param': param, 'param_string': param_string
        }
        raise exception.InvalidParameterValue(err=msg)
    return strutils.bool_from_string(param, strict=True)


def sanitize_hostname(hostname):
    """Return a hostname which conforms to RFC-952 and RFC-1123 specs."""
    if six.PY3:
        hostname = hostname.encode('latin-1', 'ignore')
        hostname = hostname.decode('latin-1')
    else:
        if isinstance(hostname, six.text_type):
            hostname = hostname.encode('latin-1', 'ignore')

    hostname = re.sub('[ _]', '-', hostname)
    hostname = re.sub('[^\w.-]+', '', hostname)
    hostname = hostname.lower()
    hostname = hostname.strip('.-')
    return hostname


def walk_class_hierarchy(clazz, encountered=None):
    """Walk class hierarchy, yielding most derived classes first."""
    if not encountered:
        encountered = []
    for subclass in clazz.__subclasses__():
        if subclass not in encountered:
            encountered.append(subclass)
            # drill down to leaves first
            for subsubclass in walk_class_hierarchy(subclass, encountered):
                yield subsubclass
            yield subclass
