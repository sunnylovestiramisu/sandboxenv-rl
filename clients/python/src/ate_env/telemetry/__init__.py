# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Optional telemetry for the ate_env client.

Tracing needs the ``tracing`` extra and client metrics the ``metrics`` extra.
Nothing here imports OpenTelemetry or prometheus_client at import time;
without the extras every call is a no-op.
"""

from .metrics import enable_client_metrics
from .tracing import (
    CLIENT_SPAN_PREFIX,
    TraceContextInterceptor,
    TracerManager,
    create_tracer_manager,
    initialize_tracer,
    inject_metadata,
)

__all__ = [
    "CLIENT_SPAN_PREFIX",
    "TraceContextInterceptor",
    "TracerManager",
    "create_tracer_manager",
    "enable_client_metrics",
    "initialize_tracer",
    "inject_metadata",
]
