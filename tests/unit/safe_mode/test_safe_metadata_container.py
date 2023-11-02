# #
# # Copyright (c) 2023, Neptune Labs Sp. z o.o.
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #     http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.
# #
# import os
# import unittest
# from unittest.mock import patch
#
# from neptune_experimental.experimental_envs import NEPTUNE_SAFETY_MODE
# from neptune_experimental.safe_mode.safe_metadata_container import SafeMetadataContainer
#
#
# class TestMetadataContainer(SafeMetadataContainer):
#     def __init__(self) -> None:
#         super().__init__(**{"mode": "debug"})
#
#
# class TestSafeMetadataContainer(unittest.TestCase):
#     @patch.dict(os.environ, {NEPTUNE_SAFETY_MODE: "True"})
#     def test_enabled_safe_mode(self):
#         methods = [
#             '__getitem__'
#         ]
#         for method in methods:
#             with patch(f'neptune.metadata_containers.MetadataContainer.{method}') as mocked_method:
#                 mocked_method.side_effect = ValueError()
#                 getattr(TestMetadataContainer(), method)()
#
#     def test_disabled_safe_mode(self):
#         methods = [
#             '__getitem__'
#         ]
#         for method in methods:
#             with patch(f'neptune.metadata_containers.MetadataContainer.{method}') as mocked_method:
#                 mocked_method.side_effect = ValueError()
#                 with self.assertRaises(ValueError):
#                     getattr(TestMetadataContainer(), method)()
