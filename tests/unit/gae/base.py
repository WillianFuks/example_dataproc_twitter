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


import json
import unittest
import mock
import datetime
import os
import shutil


class BaseTests(object):
    config1_path = 'gae/config.py'
    config2_path = 'gae/config2.py'
    test_config = 'tests/unit/data/gae/test_config.json'
    _recover_flg = False
    _utils = None
    def prepare_environ(self):
        if os.path.isfile(self.config1_path):
            shutil.copyfile(self.config1_path, self.config2_path)
            self._recover_flg = True
            os.remove(self.config1_path)
        shutil.copyfile(self.test_config, self.config1_path)

    def clean_environ(self):
        if self._recover_flg:
            shutil.copyfile(self.config2_path, self.config1_path)
            os.remove(self.config2_path)
        else:
            os.remove(self.config1_path)
    
    @property
    def utils(self):
        if not self._utils:
            import gae.utils as utils
            self._utils = utils
        return self._utils
