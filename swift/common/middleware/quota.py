# Copyright (c) 2011 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from webob.exc import HTTPForbidden

from swift.common.utils import json, split_path, cache_from_env, get_logger
from swift.common.http import is_success
from swift.proxy.controllers.base import get_account_memcache_key, \
    get_container_memcache_key


class Quota(object):
    """
    This is a quota middleware based on configuration file.

    Add to your pipeline in proxy-server.conf, such as::

        [pipeline:main]
        pipeline = catch_errors cache tempauth quota proxy-server

    And add a quota filter section, such as::

        [filter:tempauth]
        use = egg:swift#quota
        cache_timeout = 300
        set log_name = quota
        quota = {
            "container_count": {
                "default": 5,
                "L1": 10,
                "L2": 25
            },
            "object_count": {
                "default": 200000,
                "L1": 500000,
                "L2": 1000000
            },
            "container_usage": {
                "default": 2147483648,
                "L1": 10737418240,
                "L2": 53687091200
            }
        }
    See the proxy-server.conf-sample for more information.

    :param app: The next WSGI app in the pipeline
    :param conf: The dict of configuration values
    """
    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route=conf.get('log_name', 'quota'))
        self.cache_timeout = int(conf.get('cache_timeout', 300))
        try:
            quota = json.loads(conf.get('quota') or "{}")
            self.container_count = quota['container_count']
            self.object_count = quota['object_count']
            self.container_usage = quota['container_usage']
            if not self.container_count['default']:
                raise Exception('Need default in container_count')
            if not self.object_count['default']:
                raise Exception('Need default in object_count')
            if not self.container_usage['default']:
                raise Exception('Need default in container_usage')
        except Exception, err:
            raise err

    def _get_status_int(self, response_status):
        """
        Returns the HTTP status int from the last called self._start_response
        result.
        """
        return int(response_status.split(' ', 1)[0])

    def _get_escalated_env(self, env):
        """
        Returns a new fresh WSGI environment with escalated privileges to do
        backend checks, listings, etc. that the remote user wouldn't be able
        to accomplish directly.
        """
        new_env = {'REQUEST_METHOD': 'GET',
                   'HTTP_USER_AGENT': '%s Quota' % env.get('HTTP_USER_AGENT')}
        for name in ('eventlet.posthooks', 'swift.trans_id', 'REMOTE_USER',
                     'SCRIPT_NAME', 'SERVER_NAME', 'SERVER_PORT',
                     'SERVER_PROTOCOL', 'swift.cache'):
            if name in env:
                new_env[name] = env[name]
        return new_env

    def _get_account_meta(self, env, version, account):
        """
        Get metadata of account.

        :param env: The WSGI environment
        :param version: The api version in PATH_INFO
        :param account: The name of account
        :return: tuple of (container_count, quota_level) or (None, None)
        """
        memcache_client = cache_from_env(env)
        value = None
        quota_level = None
        container_count = None
        res = [None, None, None]
        result_code = None

        def _start_response(response_status, response_headers, exc_info=None):
            res[0] = response_status
            res[1] = response_headers
            res[2] = exc_info
        # get quota_level and container_count
        account_key = get_account_memcache_key(account)
        if memcache_client:
            value = memcache_client.get(account_key)
        if value:
            self.logger.debug('value from mc: %s' % (value))
            if not isinstance(value, dict):
                result_code = value
            else:
                result_code = value.get('status')
        if is_success(result_code):
            # get from memcached
            container_count = int(value.get('container_count') or 0)
            quota_level = value.get('quota_level') or 'default'
            return container_count, quota_level
        else:
            # get from account-server
            temp_env = self._get_escalated_env(env)
            temp_env['REQUEST_METHOD'] = 'HEAD'
            temp_env['PATH_INFO'] = '/%s/%s' % (version, account)
            resp = self.app(temp_env, _start_response)
            self.logger.debug(
                'value form account-server status[%s] header[%s]' % (res[0],
                res[1]))
            result_code = self._get_status_int(res[0])
            if is_success(result_code):
                headers = dict(res[1])
                container_count = int(
                    headers.get('X-Account-Container-Count') or 0)
                quota_level = headers.get('X-Account-Meta-Quota') or 'default'
                if memcache_client:
                    memcache_client.set(
                        account_key,
                        {
                            'status': result_code,
                            'container_count': container_count,
                            'quota_level': quota_level
                        },
                        timeout=self.cache_timeout
                    )
                return container_count, quota_level
            else:
                return None, None

    def _get_container_meta(self, env, version, account, container):
        """
        Get metadata of account.

        :param env: The WSGI environment
        :param version: The api version in PATH_INFO
        :param account: The name of account
        :param container: The name of container
        :return: tuple of (container_usage, object_count) or (None, None)
        """
        memcache_client = cache_from_env(env)
        value = None
        container_usage = None
        object_count = None
        res = [None, None, None]
        result_code = None

        def _start_response(response_status, response_headers, exc_info=None):
            res[0] = response_status
            res[1] = response_headers
            res[2] = exc_info
        # get container_usage and object_count
        container_key = get_container_memcache_key(account, container)
        if memcache_client:
            value = memcache_client.get(container_key)
        if value:
            self.logger.debug('value from mc: %s' % (value))
            if not isinstance(value, dict):
                result_code = value
            else:
                result_code = value.get('status')
        if is_success(result_code):
            # get from memcached
            container_usage = int(value.get('container_usage') or 0)
            object_count = int(value.get('container_size') or 0)
            return container_usage, object_count
        else:
            temp_env = self._get_escalated_env(env)
            temp_env['REQUEST_METHOD'] = 'HEAD'
            temp_env['PATH_INFO'] = '/%s/%s/%s' % (version, account, container)
            resp = self.app(temp_env, _start_response)
            self.logger.debug(
                'value form container-server status[%s] header[%s]' % (res[0],
                res[1]))
            result_code = self._get_status_int(res[0])
            if is_success(result_code):
                headers = dict(res[1])
                container_usage = int(
                    headers.get('X-Container-Bytes-Used') or 0)
                object_count = int(
                    headers.get('X-Container-Object-Count') or 0)
                read_acl = headers.get('X-Container-Read') or ''
                write_acl = headers.get('X-Container-Write') or ''
                sync_key = headers.get('X-Container-Sync-Key') or ''
                container_version = headers.get('X-Versions-Location') or ''
                if memcache_client:
                    memcache_client.set(
                        container_key,
                        {
                            'status': result_code,
                            'read_acl': read_acl,
                            'write_acl': write_acl,
                            'sync_key': sync_key,
                            'container_size': object_count,
                            'versions': container_version,
                            'container_usage': container_usage
                        },
                        timeout=self.cache_timeout
                    )
                return container_usage, object_count
            else:
                return None, None

    def handle_quota_container(self, env, start_response, version, account,
                               container):
        """ Container Count Quota """
        container_count, quota_level = self._get_account_meta(
            env, version, account)
        try:
            quota = self.container_count[quota_level]
        except Exception:
            self.logger.warn('Invalid quota_leve %s/%s quota_level[%s].' % (
                account, container, quota_level))
            quota = None
        if quota and container_count and container_count + 1 > quota:
            self.logger.notice("Container count over quota, request[PUT %s/%s],"
                               " container_count[%s] quota[%s]" % (
                                   account, container, container_count + 1,
                                   quota))
            return HTTPForbidden(body="The number of container is over quota")(
                env, start_response)
        else:
            return self.app(env, start_response)

    def handle_quota_object(self, env, start_response, version, account,
                            container, obj):
        """ Handle quota of container usage and object count. """
        object_size = int(env.get('CONTENT_LENGTH') or 0)
        container_count, quota_level = self._get_account_meta(
            env, version, account)
        try:
            container_usage_quota = self.container_usage[quota_level]
            object_count_quota = self.object_count[quota_level]
        except Exception:
            self.logger.warn('Invalid quota_leve %s/%s quota_level[%s].' % (
                account, container, quota_level))
            container_usage_quota = None
            object_count_quota = None
        container_usage, object_count = self._get_container_meta(
            env, version, account, container)
        if container_usage_quota and (container_usage + object_size >
                                      container_usage_quota):
            self.logger.notice("Container usage over quota, "
                               "request[PUT %s/%s/%s], container_usage[%s] "
                               "object_size[%s] quota[%s]" % (
                                   account, container, obj, container_usage,
                                   object_size, container_usage_quota))
            return HTTPForbidden(body="The usage of container is over quota")(
                env, start_response)
        elif object_count_quota and object_count + 1 > object_count_quota:
            self.logger.notice("Object count over quota, request[PUT %s/%s/%s],"
                               "object_count[%s] quota[%s]" % (
                                   account, container, obj, object_count + 1,
                                   object_count_quota))
            return HTTPForbidden(body="The usage of container is over quota")(
                env, start_response)
        else:
            return self.app(env, start_response)

    def __call__(self, env, start_response):
        """
        WSGI entry point.
        Wraps env in webob.Request object and passes it down.
        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        try:
            (version, account, container, obj) = \
                split_path(env['PATH_INFO'], 1, 4, True)
        except ValueError:
            return self.app(env, start_response)
        if env['REQUEST_METHOD'] == 'PUT':
            if not obj and container:
                return self.handle_quota_container(
                    env, start_response, version, account, container)
            if obj and container:
                return self.handle_quota_object(
                    env, start_response, version, account, container, obj)
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def quota_filter(app):
        return Quota(app, conf)
    return quota_filter
