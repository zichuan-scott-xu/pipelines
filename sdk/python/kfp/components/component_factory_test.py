# Copyright 2022 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from kfp.components import component_factory
from kfp.components import placeholders


class TestGetPackagesToInstallCommand(unittest.TestCase):

    def test_with_no_packages_to_install(self):
        packages_to_install = []

        command = component_factory._get_packages_to_install_command(
            packages_to_install)
        self.assertEqual(command, [])

    def test_with_packages_to_install_and_no_pip_index_url(self):
        packages_to_install = ['package1', 'package2']

        command = component_factory._get_packages_to_install_command(
            packages_to_install)
        concat_command = ' '.join(command)
        for package in packages_to_install:
            self.assertTrue(package in concat_command)

    def test_with_packages_to_install_with_pip_index_url(self):
        packages_to_install = ['package1', 'package2']
        pip_index_urls = ['https://myurl.org/simple']

        command = component_factory._get_packages_to_install_command(
            packages_to_install, pip_index_urls)
        concat_command = ' '.join(command)
        for package in packages_to_install + pip_index_urls:
            self.assertTrue(package in concat_command)


class TestContainerComponentArtifactChannel(unittest.TestCase):

    def test_correct_placeholder_and_attribute_error(self):
        in_channel = component_factory.ContainerComponentArtifactChannel(
            'input', 'my_dataset')
        out_channel = component_factory.ContainerComponentArtifactChannel(
            'output', 'my_result')
        self.assertEqual(in_channel.uri,
                         placeholders.InputUriPlaceholder('my_dataset'))
        self.assertEqual(out_channel.path,
                         placeholders.OutputPathPlaceholder('my_result'))
        self.assertRaisesRegex(AttributeError,
                               r'Cannot access artifact attribute "name"',
                               lambda: in_channel.name)
        self.assertRaisesRegex(AttributeError,
                               r'Cannot access artifact attribute "channel"',
                               lambda: out_channel.channel)
