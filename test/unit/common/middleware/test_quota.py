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
from webob import Response
from contextlib import contextmanager

from swift.common.middleware import quota

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
        self.conf = {'quota': QUOTA}

    def test_app_set(self):
        app = FakeApp()
        qa = quota.filter_factory(self.conf)(app)
        self.assertEquals(qa.app, app)

    def test_cache_timeout_set(self):
        self.conf['cache_timeout'] = 500
        qa = quota.filter_factory(self.conf)(FakeApp())
        self.assertEquals(qa.cache_timeout, 500)

    def test_cache_timeout_unset(self):
        qa = quota.filter_factory(self.conf)(FakeApp())
        self.assertEquals(qa.cache_timeout, 300)

    def test_invalid_path(self):
        pass

if __name__ == '__main__':
    unittest.main()
