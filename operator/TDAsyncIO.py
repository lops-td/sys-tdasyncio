"""AsyncIOManager - A general-purpose asyncio task manager
Based on TDAsyncIO by Motoki Sonoda
"""

import asyncio
import time
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable
from TDStoreTools import StorageManager
import traceback

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class AsyncIOTask:
    """Represents a tracked asyncio task"""
    
    def __init__(self, task_id, coroutine, description=None, info=None, timeout=None, completion_callback=None):
        self.task_id = task_id
        self.coroutine = coroutine
        self.task = None
        self.description = description
        self.info = info
        self.timeout = timeout
        self.completion_callback = completion_callback
        self.status = TaskStatus.PENDING
        self.created_at = time.time()
        self.completed_at = None
        self.result = None
        self.error = None

class AsyncIOManager:
    """
    A general-purpose asyncio manager (inspired by TDAsyncIO by Motoki Sonoda)
    """
    def __init__(self, ownerComp):
        self.ownerComp = ownerComp
        self.logger = self.ownerComp.op('Logger').ext.Logger
        self.logger.log('AsyncIOManager initialized', 'AsyncIOManager initialized', 'INFO')
        # Get the current event loop
        self.loop = asyncio.get_event_loop()
        
        # If the current event loop was closed, create a new one
        if self.loop.is_closed():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
        
        # Task tracking
        self.tasks = {}
        self.task_counter = 0
        self.frame = 0
        
        # Set up the task monitoring table
        self._setup_task_table()
    
    def _setup_task_table(self):
        """Set up the table for tracking asyncio tasks"""
        table = self.ownerComp.op('task_table')
        if not table:
            table = self.ownerComp.create(tableDAT, 'task_table')
            table.clear()
            # Reordered: task_id, status, description, duration, then the rest
            headers = ['task_id', 'status', 'description', 'duration', 'created_at', 'completed_at', 'error', 'info']
            table.appendRow(headers)
    
    def _update_task_table(self):
        if not self.ownerComp.par.Updatetable.eval():
            return
        """Update the task tracking table"""
        table = self.ownerComp.op('task_table')
        if table:
            table.clear(keepFirstRow=True)
            for task_id, task in self.tasks.items():
                duration = None
                if task.completed_at:
                    duration = round(task.completed_at - task.created_at, 3)
                elif task.status != TaskStatus.CANCELLED:
                    duration = round(time.time() - task.created_at, 3)
                
                # Reordered: task_id, status, description, duration, then the rest
                table.appendRow([
                    str(task_id),
                    task.status.value,
                    task.description or "",
                    str(duration) if duration is not None else "",
                    time.strftime("%H:%M:%S", time.localtime(task.created_at)),
                    time.strftime("%H:%M:%S", time.localtime(task.completed_at)) if task.completed_at else "",
                    str(task.error) if task.error else "",
                    self._get_task_info_str(task) # Use new helper method
                ])

    def _get_task_info_str(self, task_obj):
        """Safely get the string representation of task.info"""
        try:
            if hasattr(task_obj, 'info') and task_obj.info is not None:
                return str(task_obj.info)
            return ""
        except Exception as e:
            print(f"AsyncIOManager: Error converting task.info to string for task_id {getattr(task_obj, 'task_id', 'UNKNOWN')}. Error: {e}, Info type: {type(getattr(task_obj, 'info', None))}")
            return "Error in info"

    async def _task_wrapper(self, asyncio_task: AsyncIOTask):
        """
        Wrap the coroutine. Only sets status to RUNNING. 
        The final status is determined in the Update loop by inspecting the task object.
        """
        try:
            asyncio_task.status = TaskStatus.RUNNING
            
            coro_to_await = asyncio_task.coroutine[0] if isinstance(asyncio_task.coroutine, list) and len(asyncio_task.coroutine) == 1 else asyncio_task.coroutine
            if not asyncio.iscoroutine(coro_to_await):
                 raise TypeError(f"Expected a coroutine, but got {type(coro_to_await).__name__}")
            
            self.logger.log(f"Task {asyncio_task.task_id} ({asyncio_task.description}): Awaiting coroutine.", 'TDAsyncIO', level='DEBUG')
            
            # Await the actual work
            result = await coro_to_await
            asyncio_task.result = result
            return result
        except asyncio.CancelledError:
            # Re-raise so the asyncio.Task's state is correctly set to 'cancelled'
            self.logger.log(f"Task {asyncio_task.task_id} ({asyncio_task.description}): Coroutine was cancelled.", 'TDAsyncIO', level='WARNING')
            raise
        except Exception as e:
            # Store the exception and re-raise it so the asyncio.Task's state is correctly set to 'faulted'
            self.logger.log(f"Task {asyncio_task.task_id} ({asyncio_task.description}): Coroutine raised an exception: {e}\n{traceback.format_exc()}", 'TDAsyncIO', level='ERROR')
            asyncio_task.error = e
            raise
        finally:
            # Mark the time, but do not change the status here.
            asyncio_task.completed_at = time.time()
            self.logger.log(f"Task {asyncio_task.task_id} ({asyncio_task.description}): Wrapper finished.", 'TDAsyncIO', level='DEBUG')
    
    def Run(self, coroutines, description=None, info=None, timeout=None, completion_callback=None):
        """
        Run one or more coroutines as tracked tasks.

        Parameters:
        - description (str): text to display in table
        - info (dict): optional metadata to display in table
        - timeout (float): optional cancel timeout in seconds

        Accepts either a single coroutine object or an iterable (e.g., list)
        of coroutine objects.

        Returns the task_id if a single coroutine is passed, or a list of
        task_ids if multiple coroutines are passed.
        """
        task_ids = []
        last_task_id = None

        # Check if input is a single coroutine or an iterable
        if asyncio.iscoroutine(coroutines):
            coroutines_to_run = [coroutines]
        elif isinstance(coroutines, (list, tuple)):
             # Check if it's a list containing exactly one coroutine (common pattern from TDAsyncIO usage)
             if len(coroutines) == 1 and asyncio.iscoroutine(coroutines[0]):
                 coroutines_to_run = coroutines
             # Or if it's a list intended to run multiple tasks (less common for ChatTD usage)
             elif all(asyncio.iscoroutine(c) for c in coroutines):
                 coroutines_to_run = coroutines
             else:
                 raise TypeError("Iterable must contain only coroutine objects.")
        else:
             raise TypeError("Input must be a coroutine or an iterable of coroutines.")

        for coro in coroutines_to_run:
            task_id = self.task_counter
            self.task_counter += 1

            # Determine the description: Use provided, else extract from coroutine
            task_description = description
            if not task_description:
                # Try getting the code object name first
                task_description = getattr(getattr(coro, 'cr_code', None), 'co_name', None)
                # Fallback to qualified name or generic
                if not task_description:
                    task_description = getattr(coro, '__qualname__', 'Coroutine Task')

            # Use the derived description and other passed args
            asyncio_task = AsyncIOTask(task_id, coro, task_description, info, timeout, completion_callback)
            self.tasks[task_id] = asyncio_task

            # Create and store the task
            wrapped_coro = self._task_wrapper(asyncio_task)
            task = self.loop.create_task(wrapped_coro)
            asyncio_task.task = task

            # Set timeout if specified
            if timeout:
                self.loop.call_later(timeout, self._check_timeout, task_id)

            task_ids.append(task_id)
            last_task_id = task_id # Keep track of the last ID


        self._update_task_table()

        # Maintain compatibility: Return the single ID if only one task was run,
        # otherwise return the list of IDs (though ChatExt doesn't seem to use the list)
        # The original McpAsyncIO Run returned a single ID. The TDAsyncIO Run didn't explicitly return.
        # ChatExt now has a 'return call_id' which *might* rely on this return value.
        # Returning the last task_id seems the safest default if the input was a list.
        if len(task_ids) == 1:
            return task_ids[0]
        else:
             # If the original input was a list, ChatExt might expect *something*.
             # Returning the last ID is a reasonable guess.
             # Consider returning the full list `task_ids` if needed elsewhere.
            return last_task_id
    
    def _check_timeout(self, task_id):
        """Check if a task has timed out - called by the event loop itself"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            if task.status == TaskStatus.RUNNING or task.status == TaskStatus.PENDING:
                task.status = TaskStatus.TIMEOUT
                if task.task and not task.task.done():
                    task.task.cancel()
                task.error = f"Task timed out after {task.timeout} seconds"
                task.completed_at = time.time()
                # Schedule table update rather than doing it directly
                self.loop.call_soon(self._update_task_table)
    
    def CancelTask(self, task_id):
        """Cancel a specific task by ID"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            if task.task and not task.task.done():
                task.task.cancel()
                task.status = TaskStatus.CANCELLED
                task.completed_at = time.time()
                # Schedule table update rather than doing it directly
                self.loop.call_soon(self._update_task_table)
                return True
        return False
    
    def GetTaskResult(self, task_id):
        """Get the result of a completed task"""
        if task_id in self.tasks:
            task = self.tasks[task_id]
            if task.status == TaskStatus.COMPLETED:
                return task.result
        return None
    
    def cleanup_tasks(self, max_age=None):
        # Use Clearafter parameter if no explicit max_age provided
        if max_age is None:
            max_age = self.ownerComp.par.Clearafter.eval()
        current_time = time.time()
        to_remove = []
        
        for task_id, task in self.tasks.items():
            if task.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT]:
                if task.completed_at and (current_time - task.completed_at) > max_age:
                    to_remove.append(task_id)
        
        for task_id in to_remove:
            del self.tasks[task_id]
        
        if to_remove:
            # Schedule table update rather than doing it directly
            self.loop.call_soon(self._update_task_table)
    
    def Update(self):
        """
        Update the event loop (call on each frame), process completed tasks, and refresh the task table.
        """
        # Increment frame counter
        self.frame += 1

        # Advance the event loop once to allow pending coroutines to complete
        try:
            self.loop.run_until_complete(asyncio.sleep(0))
        except Exception as e:
            self.logger.log(f"Error during event loop tick: {e}", 'TDAsyncIO', level='ERROR', exc_info=True)

        # After loop iteration, scan for tasks that have finished
        completed_task_ids = []
        for task_id, task in self.tasks.items():
            # Check if the underlying asyncio task is done and we haven't processed it yet
            if task.task and task.task.done() and task.status in [TaskStatus.PENDING, TaskStatus.RUNNING]:
                self.logger.log(f"Update: Found completed raw task: {task_id} ({task.description})", 'TDAsyncIO', level='INFO')
                # The rest of the completion logic and callback will be handled below.
                completed_task_ids.append(task_id)

        if completed_task_ids:
            self.logger.log(f"Update: Processing {len(completed_task_ids)} newly completed tasks.", 'TDAsyncIO', level='INFO')

        for task_id in completed_task_ids:
            task = self.tasks.get(task_id)
            if not task: continue

            # Task has just finished, determine final status
            current_task_final_status_determined = False
            try:
                if task.task.cancelled():
                    task.status = TaskStatus.CANCELLED
                    current_task_final_status_determined = True
                elif task.task.exception():
                    task.status = TaskStatus.FAILED
                    # Error is already set in _task_wrapper if it came from there.
                    # If it's a new kind of exception (e.g. InvalidStateError), set it.
                    if not task.error: task.error = str(task.task.exception())
                    current_task_final_status_determined = True
                else:
                    task.status = TaskStatus.COMPLETED
                    # Result is already set in _task_wrapper if it came from there.
                    if task.result is None: task.result = task.task.result() # Get result if not already set
                    current_task_final_status_determined = True
            except (asyncio.CancelledError, asyncio.InvalidStateError) as e:
                # This handles cases where checking .exception() or .result() itself raises (e.g., if task was cancelled mid-check)
                if not task.status == TaskStatus.CANCELLED: # Avoid double setting if already cancelled
                    task.status = TaskStatus.CANCELLED 
                    self.ownerComp.op('Logger').ext.Logger.log(f"Task {task.task_id} state error during finalization: {e}", level='DEBUG')
                current_task_final_status_determined = True
            except Exception as e:
                # Catch any other unexpected error during finalization
                task.status = TaskStatus.FAILED
                if not task.error: task.error = f"Error during task finalization: {str(e)}"
                self.ownerComp.op('Logger').ext.Logger.log(f"Task {task.task_id} unexpected finalization error: {e}", level='ERROR')
                current_task_final_status_determined = True
            finally:
                if not task.completed_at: # Ensure completed_at is set
                    task.completed_at = time.time()

            # Re-enable callback execution
            if current_task_final_status_determined and callable(task.completion_callback):
                try:
                    self.logger.log(f"Update: Invoking completion callback for task {task.task_id} ({task.description})", 'TDAsyncIO', level='INFO')
                    # Call the callback with the task object itself
                    task.completion_callback(task)
                    self.logger.log(f"Update: Completion callback for task {task.task_id} finished.", 'TDAsyncIO', level='DEBUG')
                except Exception as callback_e:
                    # Log error from the callback itself but don't let it crash the manager
                    error_msg = f"Error in completion_callback for task {task.task_id} ({task.description}): {callback_e}\n{traceback.format_exc()}"
                    self.logger.log(error_msg, level='ERROR')
                    # Optionally, mark the task as failed if the callback fails, or add a special status?
                    # For now, just log the callback error. The task's primary status remains.

        # Refresh the task table every frame so completed/updated statuses appear immediately
        self._update_task_table()

        # Clean up old tasks periodically (every 100 frames)
        if self.frame % 100 == 0:
            self.cleanup_tasks()

    def Clearall(self):
        """Cancel and remove all tracked tasks, then refresh the task table."""
        self.Cancelactive()
        self.tasks.clear()
        self._update_task_table()

    def Clearfinished(self):
        """Remove only finished (Completed, Failed, Cancelled, Timeout) tasks from tracking."""
        finished_statuses = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT}
        to_remove = [tid for tid, t in self.tasks.items() if t.status in finished_statuses]
        for tid in to_remove:
            self.tasks.pop(tid, None)
        self._update_task_table()

    def Cancelactive(self):
        """Cancel all currently active (Pending or Running) tasks, leaving them in the table."""
        for task_id, task in self.tasks.items():
            if task.status in [TaskStatus.PENDING, TaskStatus.RUNNING]:
                self.CancelTask(task_id)
    
    def GetTaskStatus(self):
        """
        Public method to get the TaskStatus enum for use by other extensions.
        This avoids import issues in TouchDesigner's execution context.
        """
        return TaskStatus
    
    def GetTaskStatusValues(self):
        """
        Get all TaskStatus enum values as a dictionary for easy reference.
        Returns: dict with status names as keys and enum values as values
        """
        return {
            'PENDING': TaskStatus.PENDING,
            'RUNNING': TaskStatus.RUNNING,
            'COMPLETED': TaskStatus.COMPLETED,
            'FAILED': TaskStatus.FAILED,
            'CANCELLED': TaskStatus.CANCELLED,
            'TIMEOUT': TaskStatus.TIMEOUT
        }
    
    def GetTaskInfo(self, task_id):
        """
        Get detailed information about a specific task.
        Returns: dict with task information or None if task not found
        """
        if task_id in self.tasks:
            task = self.tasks[task_id]
            return {
                'task_id': task.task_id,
                'description': task.description,
                'status': task.status.value,
                'created_at': task.created_at,
                'completed_at': task.completed_at,
                'timeout': task.timeout,
                'error': task.error,
                'has_result': task.result is not None,
                'info': task.info
            }
        return None
    
    def GetAllTasksInfo(self):
        """
        Get information about all tracked tasks.
        Returns: list of task info dictionaries
        """
        return [self.GetTaskInfo(task_id) for task_id in self.tasks.keys()]
    
    def GetActiveTasksCount(self):
        """
        Get count of currently active (pending or running) tasks.
        Returns: int count of active tasks
        """
        active_statuses = {TaskStatus.PENDING, TaskStatus.RUNNING}
        return sum(1 for task in self.tasks.values() if task.status in active_statuses)

    def __del__(self):
        """Clean up resources when the extension is destroyed"""
        # Ensure event loop is properly handled if needed in the future,
        pass
