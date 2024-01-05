from django.apps import apps
import importlib.util
import inspect

# 定义任务模型
class Task:
    def __init__(self, name, func, docs):
        self.name = name
        self.func = func
        self.docs = docs

# 存储任务列表
task_list = []

# 遍历已注册的应用并发现任务
def discover_tasks():
    registered_apps = apps.get_app_configs()
    for app in registered_apps:
        try:
            tasks_module = importlib.import_module(f"{app.name}.tasks")
        except ModuleNotFoundError:
            continue

        for task_name, task_func in inspect.getmembers(tasks_module):
            if callable(task_func) and task_name.startswith("task_"):
                task_docs = task_func.__doc__ or task_name
                task_list.append(Task(name=task_name, func=task_func, docs=task_docs))
    # print(task_list)

def task_func(name):
    """获取任务列表中的任务"""
    for task in task_list:
        if task.name == name:
            return task.func
    return None


discover_tasks()


