---
title: TDAsyncIO
description: Manages an asyncio event loop within TouchDesigner's frame cycle, allowing operators to run asynchronous Python tasks without blocking the UI.
related: [chattd]
components: [Aside]
docs_updated_at_commit: d618f39
---

<Aside type="note" title="Acknowledgement">
This component is based on the original TDAsyncIO created by Motoki Sonoda ([sndmtk/TouchDesigner-asyncio](https://github.com/sndmtk/TouchDesigner-asyncio)) and adapted for use within the LOPs framework.
</Aside>

## Overview

TDAsyncIO provides a managed asyncio event loop that advances incrementally each TouchDesigner frame. It allows any LOPs operator to submit long-running Python coroutines -- such as network requests, file I/O, or heavy computation -- and have them execute without freezing the TouchDesigner interface. TDAsyncIO lives inside ChatTD and is automatically available to every operator that inherits from DotLOPUtils via `self.asyncio_comp`.

## Key Concepts

- **Frame-Driven Event Loop** -- Each frame, TDAsyncIO runs one iteration of the asyncio event loop, advancing all pending coroutines. No background threads are needed for the loop itself.
- **Tracked Tasks** -- Every coroutine submitted through `Run` becomes a tracked task with a unique ID, status, timing information, and optional metadata.
- **Task Lifecycle** -- Tasks progress through statuses: pending, running, completed, failed, cancelled, or timeout.
- **Task Table** -- An internal `task_table` DAT provides real-time visibility into all managed tasks, showing ID, status, description, duration, timestamps, errors, and custom metadata.
- **Completion Callbacks** -- Each task can carry a callback function that fires automatically on the main thread when the task finishes, making it safe to update TouchDesigner state from the callback.
- **Automatic Cleanup** -- Finished tasks are automatically removed from the task table after a configurable age, preventing unbounded table growth.

## How Operators Use TDAsyncIO

Most operators never interact with TDAsyncIO directly. Instead, they call `self.run_async_task()` inherited from DotLOPUtils, which handles submitting coroutines and polling for results. The typical flow is:

1. An operator defines an `async def` method for its long-running work (e.g., an API call).
2. The operator calls `self.run_async_task([coroutine], description="...", completion_callback=handler)`.
3. DotLOPUtils forwards this to TDAsyncIO's `Run` method.
4. TDAsyncIO tracks the task and advances it each frame.
5. When the task finishes, TDAsyncIO invokes the completion callback on the main thread, where it is safe to update parameters, DATs, and other TouchDesigner objects.

## Task Statuses

| Status | Meaning |
|---|---|
| pending | Task has been submitted but has not started executing yet |
| running | Task coroutine is actively being awaited |
| completed | Task finished successfully and its result is available |
| failed | Task raised an exception during execution |
| cancelled | Task was manually cancelled before completion |
| timeout | Task exceeded its timeout duration and was automatically cancelled |

## Task Table

The `task_table` DAT inside TDAsyncIO displays all tracked tasks with the following columns:

- **task_id** -- Unique numeric identifier
- **status** -- Current lifecycle state
- **description** -- Human-readable label provided when the task was submitted
- **duration** -- Elapsed time in seconds (updates while running, final value when finished)
- **created_at** -- Time the task was submitted
- **completed_at** -- Time the task finished (blank while still active)
- **error** -- Error message if the task failed
- **info** -- Custom metadata dictionary passed when the task was submitted

## Thread Safety

<Aside type="danger" title="Critical Rule">
Functions running inside worker threads (via `asyncio.to_thread` or `loop.run_in_executor`) must NEVER call TouchDesigner APIs. No `op()`, no `par`, no `ui`, no DAT modifications, no logger calls. Violating this rule causes thread conflicts and crashes.
</Aside>

When a coroutine offloads blocking work to a worker thread, that thread operates outside TouchDesigner's main context. All TouchDesigner interactions must happen either:

- In the async coroutine itself (after `await` returns from the worker thread)
- In the `completion_callback`, which TDAsyncIO always fires on the main thread

## Troubleshooting

- **TouchDesigner freezes during async work** -- The coroutine is likely calling a blocking function directly instead of offloading it with `await asyncio.to_thread(...)`. Wrap blocking calls so the event loop can continue advancing.
- **Thread conflict warnings or crashes** -- A worker thread is calling TouchDesigner APIs. Move all TD interactions out of the worker function and into the coroutine or completion callback.
- **Task stuck in "running" forever** -- The awaited coroutine may be deadlocked or waiting on a resource that will never resolve. Use the timeout parameter when submitting tasks, or pulse Cancel Active Tasks to force cancellation.
- **Task table growing very large** -- Lower the Clear After value to remove finished tasks sooner, or pulse Clear Finished Tasks to remove them immediately.
