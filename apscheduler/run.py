from django_apscheduler.apps import DjangoApschedulerConfig
from apscheduler.schedulers.background import BackgroundScheduler
from django_apscheduler.jobstores import DjangoJobStore, register_events, register_job
from django.conf import settings
import time




from django_apscheduler.models import DjangoJobExecution,DjangoJob

scheduler=None
try:

    scheduler = BackgroundScheduler(timezone=settings.TIME_ZONE)	# timezone是用来设置时区的
    scheduler.add_jobstore(DjangoJobStore(), "default")
    def reg_job(*args, **kwargs) -> callable:
        def wrapper_register_job(func):
            kwargs.setdefault("id", f"{func.__module__}.{func.__name__}")
            scheduler.add_job(func, *args, **kwargs)
            return func
        return wrapper_register_job
    # 'cron', hour = "23", minute = "59"  周期执行
    # scheduler.add_job(func=print, trigger="cron", hour="*/1", minute="*/1", id="test")
    # @register_job(scheduler, 'interval', seconds=3)   # 每隔3s执行一次
    # def text():
    #     time.sleep(4)   # 间隔4s输出，测试多线程
    #     print("我是apscheduler定时任务")
    # # register_events(scheduler)	
    scheduler.start()
    # 调用发现任务函数
    
    print("调度程序任务成功启动！")
except Exception as e:
    print("定时服务错误,已关闭:%s" % e)
    if scheduler:
        scheduler.shutdown()
