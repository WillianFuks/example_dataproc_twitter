#MIT License
#
#Copyright (c) 2017 Willian Fuks
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.


import unittest
import mock


class TestDatastoreService(unittest.TestCase):
    @staticmethod
    def _get_target_klass():
        from gae.connector.datastore import DatastoreService


        return DatastoreService

    @mock.patch("gae.connector.datastore.ds")
    def test_cto(self, ds_mock):
        ds_mock.Client.return_value = 'client'
        klass = self._get_target_klass()('credentials')
        self.assertEqual(klass.client, 'client')
        ds_mock.Client.assert_called_once_with(credentials='credentials')

        klass = self._get_target_klass()(None)
        self.assertEqual(klass.client, 'client')

    @mock.patch("gae.connector.datastore.ds")
    def test_get_keys(self, ds_mock):
        client_mock = mock.Mock
        ds_mock.Client.return_value = client_mock
        klass = self._get_target_klass()(None)
                                














