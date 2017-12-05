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


import datetime
import os
import shutil


class BaseTest(object):
    @classmethod
    def get_paths(cls, path, s):
        for i in range(1, s):
            source_path = path.format(i)
            date_str = (datetime.datetime.now() 
                        + datetime.timedelta(days=-i)).strftime("%Y-%m-%d")
            dest_path = path.format(date_str)  
            yield [source_path, dest_path]
 
    @classmethod
    def build_data(cls, path):
        for source_path, dest_path in cls.get_paths(path, 3):
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
                shutil.copyfile(source_path + 'result.gz',
                    dest_path + 'result.gz')

    @classmethod 
    def delete_data(cls, path):
        for _, dest in cls.get_paths(path, 3):
            dir_ = os.path.dirname(dest)
            if os.path.isdir(dir_):
                shutil.rmtree(dir_)
