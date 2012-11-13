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

import unittest
from webob import Response, Request
from contextlib import contextmanager

from swift.common.middleware import quota
from swift.common.utils import json

QUOTA = '{"object_count": {"default": 200000, "L2": 1000000, "L1": 500000}, "container_count": {"default": 5, "L2": 25, "L1": 10}, "container_usage": {"default": 2147483648, "L1": 5368709120, "L2": 10737418240}}'
QUOTA_E1 = '{"object_count": {"default": 200000}, "container_count": {}, "container_usage": {"default": 2147483648}}'
QUOTA_E2 = '{"object_count": {}, "container_count": {"default": 5}, "container_usage": {"default": 2147483648}}'
QUOTA_E3 = '{"object_count": {"default": 200000}, "container_count": {"default": 5}, "container_usage": {}}'
QUOTA_E4 = '{"object_count": {"default": 200000}, "container_usage": {"default": 2147483648}}'
QUOTA_E5 = '{"container_count": {"default": 5}, "container_usage": {"default": 2147483648}}'
QUOTA_E6 = '{"object_count": {"default": 200000}, "container_count": {"default": 5}}'


class FakeMemcache(object):

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, timeout=0):
        self.store[key] = value
        return True

    def incr(self, key, timeout=0):
        self.store[key] = self.store.setdefault(key, 0) + 1
        return self.store[key]

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except Exception:
            pass
        return True


class FakeApp(object):
    def __init__(self):
        pass

    def __call__(self, env, start_response):
        if env['PATH_INFO'] == '/':
            return Response(status='404 Not Found')(env, start_response)
        elif env['PATH_INFO'] == '/v1':
            return Response(
                status='412 Precondition Failed')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a':
            return Response(status='401 Unauthorized')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a-c2-qdefault':
            headers = {
                'x-account-container-count': 2,
                'x-account-object-count': 100,
                'x-account-bytes-used': 1024,
            }
            return Response(status='200 OK', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a-c2-qdefault/c':
            return Response(status='201 Created')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a-c4-qdefault':
            headers = {
                'x-account-container-count': 4,
                'x-account-object-count': 100,
                'x-account-bytes-used': 1024,
            }
            return Response(status='200 OK', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a-c4-qdefault/c':
            return Response(status='201 Created')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a-c5-qdefault':
            headers = {
                'x-account-container-count': 5,
                'x-account-object-count': 100,
                'x-account-bytes-used': 1024,
            }
            return Response(status='200 OK', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a-c8-ql1':
            headers = {
                'x-account-container-count': 8,
                'x-account-object-count': 100,
                'x-account-bytes-used': 1024,
                'x-account-meta-quota': 'L1',
            }
            return Response(status='200 OK', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a-c8-ql1/c':
            return Response(status='201 Created')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a1':
            headers = {
                'x-account-container-count': 2,
                'x-account-object-count': 100,
                'x-account-bytes-used': 1024,
            }
            return Response(status='200 OK', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a1/c':
            headers = {
                'x-container-object-count': 100,
                'x-container-bytes-used': 200,
            }
            return Response(status='204 No Content', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a1/c/obj':
            return Response(status='201 Created')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a1/c/obj1':
            return Response(status='500 Internal Server Error')(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a1/c2':
            headers = {
                'x-container-object-count': 100,
                'x-container-bytes-used': 2147483648 - 1024,
            }
            return Response(status='204 No Content', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a1/c2/obj':
            return Response(status='201 Created')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a1/c3':
            headers = {
                'x-container-object-count': 100,
                'x-container-bytes-used': 2147483648 + 1024,
            }
            return Response(status='204 No Content', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a2':
            headers = {
                'x-account-container-count': 2,
                'x-account-object-count': 100,
                'x-account-bytes-used': 1024,
                'x-account-meta-quota': 'L1'
            }
            return Response(status='200 OK', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a2/c':
            headers = {
                'x-container-object-count': 100,
                'x-container-bytes-used': 2147483648,
            }
            return Response(status='204 No Content', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a2/c/obj':
            return Response(status='201 Created')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a3':
            headers = {
                'x-account-container-count': 2,
                'x-account-object-count': 100,
                'x-account-bytes-used': 1024,
            }
            return Response(status='200 OK', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a3/c':
            headers = {
                'x-container-object-count': 100,
                'x-container-bytes-used': 1024,
            }
            return Response(status='204 No Content', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a3/c/obj':
            return Response(status='201 Created')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a3/c1':
            headers = {
                'x-container-object-count': 99999,
                'x-container-bytes-used': 1024,
            }
            return Response(status='204 No Content', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a3/c1/obj':
            return Response(status='201 Created')(env, start_response)
        elif env['PATH_INFO'] == '/v1/a3/c2':
            headers = {
                'x-container-object-count': 200000,
                'x-container-bytes-used': 1024,
            }
            return Response(status='204 No Content', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a4':
            headers = {
                'x-account-container-count': 2,
                'x-account-object-count': 100,
                'x-account-bytes-used': 1024,
                'x-account-meta-quota': 'L1',
            }
            return Response(status='200 OK', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a4/c':
            headers = {
                'x-container-object-count': 100050,
                'x-container-bytes-used': 1024,
            }
            return Response(status='204 No Content', headers=headers)(
                env, start_response)
        elif env['PATH_INFO'] == '/v1/a4/c/obj':
            return Response(status='201 Created')(env, start_response)


class TestReadConfiguration(unittest.TestCase):
    def test_read_conf(self):
        quota_filter = quota.filter_factory({"quota": QUOTA})(FakeApp())
        self.assertNotEqual(quota_filter.app, None)
        self.assertEqual(quota_filter.container_count['default'], 5)
        self.assertEqual(quota_filter.container_count['L1'], 10)
        self.assertEqual(quota_filter.container_count['L2'], 25)
        self.assertEqual(quota_filter.object_count['default'], 200000)
        self.assertEqual(quota_filter.object_count['L1'], 500000)
        self.assertEqual(quota_filter.object_count['L2'], 1000000)
        self.assertEqual(quota_filter.container_usage['default'], 2147483648)
        self.assertEqual(quota_filter.container_usage['L1'], 5368709120)
        self.assertEqual(quota_filter.container_usage['L2'], 10737418240)

    def test_read_conf_e(self):
        try:
            quota_filter = quota.filter_factory({"quota": QUOTA_E1})(FakeApp())
        except Exception:
            self.assertTrue(True)
        else:
            self.fail("No specific options.")

        try:
            quota_filter = quota.filter_factory({"quota": QUOTA_E2})(FakeApp())
        except Exception:
            self.assertTrue(True)
        else:
            self.fail("No specific options.")

        try:
            quota_filter = quota.filter_factory({"quota": QUOTA_E3})(FakeApp())
        except Exception:
            self.assertTrue(True)
        else:
            self.fail("No specific options.")

        try:
            quota_filter = quota.filter_factory({"quota": QUOTA_E4})(FakeApp())
        except Exception:
            self.assertTrue(True)
        else:
            self.fail("No specific options.")

        try:
            quota_filter = quota.filter_factory({"quota": QUOTA_E5})(FakeApp())
        except Exception:
            self.assertTrue(True)
        else:
            self.fail("No specific options.")

        try:
            quota_filter = quota.filter_factory({"quota": QUOTA_E6})(FakeApp())
        except Exception:
            self.assertTrue(True)
        else:
            self.fail("No specific options.")


class TestQuota(unittest.TestCase):
    def setUp(self):
        self.conf = {
            'quota': QUOTA,
            'log_name': 'quota',
            'cache_timeout': 500,
            'log_level': 'DEBUG',
        }

    def test_app_set(self):
        app = FakeApp()
        qa = quota.filter_factory(self.conf)(app)
        self.assertEquals(qa.app, app)

    def test_logger_set(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        self.assertEquals(qa.logger.server, 'quota')

    def test_cache_timeout_set(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        self.assertEquals(qa.cache_timeout, 500)

    def test_cache_timeout_unset(self):
        self.conf.pop('cache_timeout')
        qa = quota.filter_factory(self.conf)(FakeApp())
        self.assertEquals(qa.cache_timeout, 300)

    def test_invalid_path(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        resp = Request.blank('/').get_response(qa)
        self.assertEquals(resp.status_int, 404)
        resp = Request.blank('/v1').get_response(qa)
        self.assertEquals(resp.status_int, 412)
        resp = Request.blank('/v1/a').get_response(qa)
        self.assertEquals(resp.status_int, 401)

    def test_container_count_quota_default_ok(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a-c2-qdefault/c')
        req.method = 'PUT'
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)

    def test_container_count_quota_default_boundary(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a-c4-qdefault/c')
        req.method = 'PUT'
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)

    def test_container_count_quota_default_fail(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a-c5-qdefault/c')
        req.method = 'PUT'
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The number of container is over quota')
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The number of container is over quota')
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The number of container is over quota')

    def test_container_count_quota_l1_ok(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a-c8-ql1/c')
        req.method = 'PUT'
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)

    def test_container_count_quota_error_key(self):
        self.conf['quota'] = json.dumps(
            {
                'container_count': {
                    'default': 5,
                    'L10': 10
                },
                'object_count': {
                    'default': 5,
                    'L10': 10
                },
                'container_usage': {
                    'default': 5,
                    'L10': 10
                }
            }
        )
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a-c8-ql1/c')
        req.method = 'PUT'
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)

    def test_container_usage_default_ok(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a1/c/obj')
        req.method = 'PUT'
        req.environ['CONTENT_LENGTH'] = 1024
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        ###############
        # test 500
        ###############
        req = Request.blank('/v1/a1/c/obj1')
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 500)

    def test_container_usage_default_boundary(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a1/c2/obj')
        req.method = 'PUT'
        req.environ['CONTENT_LENGTH'] = 1024
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)

    def test_container_usage_default_fail(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a1/c2/obj')
        req.environ['CONTENT_LENGTH'] = 1025
        req.method = 'PUT'
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The usage of container is over quota')
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The usage of container is over quota')
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The usage of container is over quota')
        ########################
        # already over quota
        ########################
        req = Request.blank('/v1/a1/c3/obj')
        req.method = 'PUT'
        req.environ['CONTENT_LENGTH'] = 1023
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The usage of container is over quota')
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The usage of container is over quota')
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The usage of container is over quota')

    def test_container_usage_l1_ok(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a2/c/obj')
        req.method = 'PUT'
        req.environ['CONTENT_LENGTH'] = 1024
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)

    def test_container_usage_error_key(self):
        self.conf['quota'] = json.dumps(
            {
                'container_count': {
                    'default': 5,
                    'L10': 10
                },
                'object_count': {
                    'default': 5,
                    'L10': 10
                },
                'container_usage': {
                    'default': 5,
                    'L10': 10
                }
            }
        )
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a2/c/obj')
        req.method = 'PUT'
        req.environ['CONTENT_LENGTH'] = 1024
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)

    def test_object_count_default_ok(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a3/c/obj')
        req.method = 'PUT'
        req.environ['CONTENT_LENGTH'] = 1024
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)

    def test_object_count_default_boundary(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a3/c1/obj')
        req.method = 'PUT'
        req.environ['CONTENT_LENGTH'] = 1024
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)

    def test_object_count_default_fail(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a3/c2/obj')
        req.method = 'PUT'
        req.environ['CONTENT_LENGTH'] = 1024
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The count of object is over quota')
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The count of object is over quota')
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(resp.body, 'The count of object is over quota')

    def test_object_count_l1_ok(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        req = Request.blank('/v1/a4/c/obj')
        req.method = 'PUT'
        req.environ['CONTENT_LENGTH'] = 1024
        # no memcached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # no cache
        req.environ['swift.cache'] = FakeMemcache()
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)
        # cached
        resp = req.get_response(qa)
        self.assertEquals(resp.status_int, 201)


if __name__ == '__main__':
    unittest.main()
