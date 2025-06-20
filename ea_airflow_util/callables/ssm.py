# Copyright (c) 2018 Bao Nguyen <b@nqbao.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# ==============================================================================

# Copied by Rob Little on 3/1/2022 from this github gist: https://gist.github.com/nqbao/9a9c22298a76584249501b74410b8475
# Additions and deviations made by Jay Kaiser.

import boto3
import datetime
import re
import time
from typing import Optional

from botocore.config import Config


class SSMParameterStore:
    """
    Provide a dictionary-like interface to access AWS SSM Parameter Store
    """
    TENANT_REPR = "{tenant_code}"

    def __init__(self,
        prefix     : Optional[str]  = None,
        ssm_client : Optional[str]  = None,
        region_name: Optional[str]  = None,
        ttl        : Optional[bool] = None
    ) -> None:
        self._prefix = (prefix or '').rstrip('/') + '/'
        self._keys = None
        self._substores = {}
        self._ttl = ttl

        default_config = Config(connect_timeout=5, read_timeout=60, retries={'max_attempts': 5})
        self._client = ssm_client or boto3.client('ssm', region_name=region_name, config=default_config)

    def get(self, name: str, **kwargs):
        assert name, 'Name can not be empty'
        if self._keys is None:
            self.refresh()

        if self.TENANT_REPR in self._prefix:
            abs_key = self._prefix.replace(self.TENANT_REPR, name)
        else:
            abs_key = "%s%s" % (self._prefix, name)
        
        if name not in self._keys:
            if 'default' in kwargs:
                return kwargs['default']

            raise KeyError(name)

        elif self._keys[name]['type'] == 'prefix':
            if abs_key not in self._substores:
                store = self.__class__(prefix=abs_key, ssm_client=self._client, ttl=self._ttl)
                store._keys = self._keys[name]['children']
                self._substores[abs_key] = store

            return self._substores[abs_key]

        else:
            return self._get_value(name, abs_key)


    def refresh(self):
        self._keys = {}
        self._substores = {}

        # Different logic is used when passing a complete prefix vs a wildcard.
        if self.TENANT_REPR in self._prefix:
            filter_prefix, _ = self._prefix.split(self.TENANT_REPR, 1)
        else:
            filter_prefix = self._prefix

        paginator = self._client.get_paginator('describe_parameters')
        pager = paginator.paginate(
            ParameterFilters=[
                dict(Key="Path", Option="Recursive", Values=[filter_prefix])
            ]
        )

        for page in pager:
            for p in page['Parameters']:

                # Escape uninvestigated error: AttributeError: 'NoneType' object has no attribute 'group'
                try:
                    # If a wildcard is in the prefix string, extract and set the tenant code as the first path element.
                    # Replace "{tenant_code}" in the user-prefix with a Regex wildcard capture group and extract the dynamic value.
                    # Add the inferred tenant and everything that follows it to the path keys.
                    if self.TENANT_REPR in self._prefix:
                        inferred_tenant = re.search(self._prefix.replace(self.TENANT_REPR, "(.*)"), p['Name']).group(1)
                        inferred_prefix = self._prefix.replace(self.TENANT_REPR, inferred_tenant)
                        paths = [inferred_tenant, *p['Name'][len(inferred_prefix):].split('/')]

                    else:
                        paths = p['Name'][len(self._prefix):].split('/')

                    self._update_keys(self._keys, paths)
                except:
                    continue
            
            time.sleep(1)


    @classmethod
    def _update_keys(cls, keys: dict, paths):
        name = paths[0]

        # this is a prefix
        if len(paths) > 1:
            if name not in keys:
                keys[name] = {'type': 'prefix', 'children': {}}

            cls._update_keys(keys[name]['children'], paths[1:])

        else:
            keys[name] = {'type': 'parameter', 'expire': None}


    def keys(self):
        if self._keys is None:
            self.refresh()

        return self._keys.keys()


    def _get_value(self, name, abs_key):
        entry = self._keys[name]

        # simple ttl
        if self._ttl == False or (entry['expire'] and entry['expire'] <= datetime.datetime.now()):
            entry.pop('value', None)

        if 'value' not in entry:
            parameter = self._client.get_parameter(Name=abs_key, WithDecryption=True)['Parameter']

            value = parameter['Value']
            if parameter['Type'] == 'StringList':
                value = value.split(',')

            entry['value'] = value

            if self._ttl:
                entry['expire'] = datetime.datetime.now() + datetime.timedelta(seconds=self._ttl)
            else:
                entry['expire'] = None

        return entry['value']


    def __contains__(self, name):
        try:
            self.get(name)
            return True

        except:
            return False


    def __getitem__(self, name):
        return self.get(name)

    def __setitem__(self, key, value):
        raise NotImplementedError()

    def __delitem__(self, name):
        raise NotImplementedError()

    def __repr__(self):
        return 'ParameterStore[%s]' % self._prefix
