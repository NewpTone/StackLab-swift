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

try:
    import simplejson as json
except Exception:
    import json

from webob import Request, Response
from webob.exc import HTTPForbidden

from swift.common.utils import split_path, cache_from_env, get_logger
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
        quota = {
            "container_count": {
                "default": 5,
                "L1": 10,
                "L2": 25,
            },
            "object_count": {
                "default": 200000,
                "L1": 500000,
                "L2": 1000000,
            },
            "container_usage": {
                "default": 2147483648,
                "L1": 10737418240,
                "L2": 53687091200,
            }
        }
    See the proxy-server.conf-sample for more information.

    :param app: The next WSGI app in the pipeline
    :param conf: The dict of configuration values
    """
    def __init__(self, app, conf):
        self.app = app
        try:
            quota = json.loads(conf.get('quota') or "{}")
            self.container_count = quota['container_count']
            self.object_count = quota['object_count']
            self.container_usage = quota['container_usage']
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

    def update_request(self, req):
        req.bytes_transferred = '-'
        req.client_disconnect = False
        if 'x-storage-token' in req.headers and \
                'x-auth-token' not in req.headers:
            req.headers['x-auth-token'] = req.headers['x-storage-token']
            return req

    def handle_quota_container(self, env, start_response, version, account,
                               container):
        res = [None, None, None]
        req = self.update_request(Request(env))

        def _start_response(response_status, response_headers, exc_info=None):
            res[0] = response_status
            res[1] = response_headers
            res[2] = exc_info

        tem_env = self._get_escalated_env(env)
        tem_env['REQUEST_METHOD'] = 'HEAD'
        tem_env['PATH_INFO'] = '/%s/%s' % (version, account)
        if self.app.memcache:
            cache_key = get_account_memcache_key(account)
            cache_value = self.app.memcache.get(cache_key)
            if not isinstance(cache_value, dict):
                result_code = cache_value
                container_count_num = 0
                quota_level = 'default'
            else:
                result_code = cache_value.get('status')
                container_count_num = int(
                    cache_value.get('container_count') or 0)
                quota_level = cache_value.get('quota_level') or 'default'
                if container_count_num >= int(
                        self.container_count[quota_level]):
                    #return self.app(env, start_response)
                    print container_count_num, req
                    #return HTTPForbidden(
                    #        request=req, content_type='text/plain',
                    #        body='The container count over quota.'
                    #)
                else:
                    self.app.memcache.set(
                        cache_key, {
                            'status': result_code,
                            'container_count': container_count_num + 1,
                            'quota_level': quota_level
                        }
                    )
                    return self.app(env, start_response)
        resp = self.app(tem_env, _start_response)
        result_code = self._get_status_int(res[0])
        container_count_num = int(
            dict(res[1])['x-account-container-count'] or 0)
        if 'x-account-meta-quota' not in res[1]:
            dict(res[1])['x-account-meta-quota'] = 'default'
            quota_level = 'default'
        else:
            quota_level = dict(res[1])['x-account-meta-quota'] or 'default'
        if container_count_num >= int(self.container_count[quota_level]):
            print container_count_num,
            #return HTTPForbidden(
            #        request=req, content_type='text/plain',
            #        body='The container count over quota.'
            #)
           #return self.app(env, start_response)
        else:
            cache_key = get_account_memcache_key(account)
            self.app.memcache.set(
                cache_key, {
                    'status': result_code,
                    'container_count': container_count_num + 1,
                    'quota_level': quota_level
                }
            )
            print res[1], result_code, container_count_num,\
                quota_level, self.container_count[quota_level]
            return self.app(env, start_response)

    def handle_quota_object(self, env, start_response, version, account,
                            container, obj):
        res = [None, None, None]

        def _start_response(response_status, response_headers, exc_info=None):
            res[0] = response_status
            res[1] = response_headers
            res[2] = exc_info

        tem_env = self._get_escalated_env(env)
        tem_env['REQUEST_METHOD'] = 'HEAD'
        tem_env['PATH_INFO'] = '/%s/%s/%s' % (version, account, container)
        self.app.memcache = None
        if self.app.memcache:
            cache_key = get_container_memcache_key(account, container)
            cache_value = self.app.memcache.get(cache_key)
            if not isinstance(cache_value, dict):
                result_code = cache_value
                object_count_num = 0
                container_usage_num = 0
            else:
                result_code = cache_value.get('status')
                object_count_num = int(
                    cache_value.get('object_count') or 0)
                resp = self.app(tem_env, _start_response)
                container_usage_num = int(
                    dict(res[1])['container_usage'] or 0)
                quota_level = cache_value.get('quota_level') or 'default'
                if object_count_num >= self.object_count[quota_level]:
                    return self.app(env, start_response)
                elif container_usage_num >= self.container_usage[quota_level]:
                    return self.app(env, start_response)
                else:
                    self.app.memcache.set(
                        cache_key, {
                            'status': result_code,
                            'object_count': object_count_num,
                            'container_usage': container_usage_num
                        }
                    )
                    return self.app(env, start_response)
        resp = self.app(tem_env, _start_response)
        result_code = self._get_status_int(res[0])
        object_count_num = int(
            dict(res[1])['x-container-object-count'] or 0)
        container_usage_num = int(
            dict(res[1])['x-container-bytes-used'] or 0)
        if 'quota_level' not in res[1]:
            dict(res[1])['x-account-meta-quota'] = 'default'
            quota_level = 'default'
        else:
            quota_level = dict(res[1])['x-account-meta-quota'] or 'default'
        if object_count_num >= self.object_count[quota_level]:
            return self.app(env, start_response)
        elif container_usage_num >= self.container_usage[quota_level]:
            return self.app(env, start_response)
        else:
            cache_key = get_container_memcache_key(account, container)
            self.app.memcache.set(
                cache_key, {
                    'status': result_code,
                    'object_count': object_count_num,
                    'container_usage': container_usage_num
                })
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
