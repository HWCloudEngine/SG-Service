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

"""sgservice base exception handling.

Includes decorator for re-raising sgservice-type exceptions.

SHOULD include dedicated exception logging.

"""

import sys

from oslo_config import cfg
from oslo_log import log as logging
import six
import webob.exc
from webob.util import status_generic_reasons
from webob.util import status_reasons

from sgservice.i18n import _, _LE

LOG = logging.getLogger(__name__)

exc_log_opts = [
    cfg.BoolOpt('fatal_exception_format_errors',
                default=False,
                help='Make exception message format errors fatal.'),
]

CONF = cfg.CONF
CONF.register_opts(exc_log_opts)


class ConvertedException(webob.exc.WSGIHTTPException):
    def __init__(self, code=500, title="", explanation=""):
        self.code = code
        # There is a strict rule about constructing status line for HTTP:
        # '...Status-Line, consisting of the protocol version followed by a
        # numeric status code and its associated textual phrase, with each
        # element separated by SP characters'
        # (http://www.faqs.org/rfcs/rfc2616.html)
        # 'code' and 'title' can not be empty because they correspond
        # to numeric status code and its associated text
        if title:
            self.title = title
        else:
            try:
                self.title = status_reasons[self.code]
            except KeyError:
                generic_code = self.code // 100
                self.title = status_generic_reasons[generic_code]
        self.explanation = explanation
        super(ConvertedException, self).__init__()


class Error(Exception):
    pass


class SGServiceException(Exception):
    """Base sgservice Exception

    To correctly use this class, inherit from it and define
    a 'message' property. That message will get printf'd
    with the keyword arguments provided to the constructor.

    """
    message = _("An unknown exception occurred.")
    code = 500
    headers = {}
    safe = False

    def __init__(self, message=None, **kwargs):
        """Initiate the instance of SGServiceException

        There are two ways to initiate the instance.
        1. Specify the value of 'message' and leave the 'kwargs' None.
        2. Leave 'message' None, and specify the keyword arguments matched
           with the format of SGServiceException.message. Especially, can't
           use the 'message' as the key in the 'kwargs', otherwise, the
           first argument('message') will be set.

        Note: This class doesn't support to create instance of
            SGServiceException with another instance.
        """
        self.kwargs = kwargs

        if 'code' not in self.kwargs:
            try:
                self.kwargs['code'] = self.code
            except AttributeError:
                pass

        if not message:
            try:
                message = self.message % kwargs

            except Exception:
                exc_info = sys.exc_info()
                # kwargs doesn't match a variable in the message
                # log the issue and the kwargs
                LOG.exception(_LE('Exception in string format operation'))
                for name, value in kwargs.items():
                    LOG.error(_LE("%(name)s: %(value)s"),
                              {'name': name, 'value': value})
                if CONF.fatal_exception_format_errors:
                    six.reraise(*exc_info)
                # at least get the core message out if something happened
                message = self.message
        elif isinstance(message, Exception):
            message = six.text_type(message)

        # NOTE(luisg): We put the actual message in 'msg' so that we can access
        # it, because if we try to access the message via 'message' it will be
        # overshadowed by the class' message attribute
        self.msg = message
        super(SGServiceException, self).__init__(message)

    def __unicode__(self):
        return six.text_type(self.msg)


class NotAuthorized(SGServiceException):
    message = _("Not authorized.")
    code = 403


class AdminRequired(NotAuthorized):
    message = _("User does not have admin privileges")


class Invalid(SGServiceException):
    message = _("Unacceptable parameters.")
    code = 400


class NotFound(SGServiceException):
    message = _("Resource could not be found.")
    code = 404
    safe = True


class ConfigNotFound(NotFound):
    message = _("Could not find config at %(path)s")


class PasteAppNotFound(NotFound):
    message = _("Could not load paste app '%(name)s' from %(path)s")


class InvalidContentType(Invalid):
    message = _("Invalid content type %(content_type)s.")


class MalformedRequestBody(SGServiceException):
    message = _("Malformed message body: %(reason)s")


class ServiceNotFound(NotFound):
    message = _("Service %(service_id)s could not be found.")


class HostBinaryNotFound(NotFound):
    message = _("Could not find binary %(binary)s on host %(host)s.")


class VolumeNotFound(NotFound):
    message = _("Volume %(volume_id)s could not be found.")


class ReplicationNotFound(NotFound):
    message = _("Replication %(replication_id)s could not be found.")


class SnapshotNotFound(NotFound):
    message = _("Snapshot %(snapshot_id)s could not be found.")


class BackupNotFound(NotFound):
    message = _("Backup %(backup_id)s could not be found.")


class CheckpointNotFound(NotFound):
    message = _("Checkpoint %(checkpoint_id)s could not be found.")


class InvalidUUID(Invalid):
    message = _("Expected a uuid but received %(uuid)s.")


class VolumeAttachmentNotFound(NotFound):
    message = _("Volume attachment could not be found with "
                "filter: %(filter)s .")


class InvalidInput(Invalid):
    message = _("Invalid input received: %(reason)s")


class PolicyNotAuthorized(NotAuthorized):
    message = _("Policy doesn't allow %(action)s to be performed.")
