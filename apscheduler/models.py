from datetime import timedelta, datetime

from django.db import models, transaction
from django.db.models import UniqueConstraint
from django.utils import timezone
from django.utils.translation import gettext_lazy as _


import logging
import json

from django_apscheduler import util
from django_apscheduler.util import get_django_internal_datetime

logger = logging.getLogger(__name__)


class DjangoJob(models.Model):
    id = models.CharField(
        max_length=255, primary_key=True, help_text=_("此作业的唯一ID。")
    )

    next_run_time = models.DateTimeField(
        db_index=True,
        blank=True,
        null=True,
        help_text=_(
            "计划下一次执行此作业的日期和时间。"
        ),
    )

    job_state = models.BinaryField()

    def __str__(self):
        status = (
            f"下次运行: {util.get_local_dt_format(self.next_run_time)}"
            if self.next_run_time
            else "暂停"
        )
        return f"{self.id} ({status})"

    class Meta:
        ordering = ("next_run_time",)
        verbose_name = "任务一览"
        verbose_name_plural = verbose_name

class DjangoJobExecutionManager(models.Manager):
    def delete_old_job_executions(self, max_age: int):
        """
        Delete old job executions from the database.

        :param max_age: The maximum age (in seconds). Executions that are older
        than this will be deleted.
        """
        self.filter(run_time__lte=timezone.now() - timedelta(seconds=max_age)).delete()


class DjangoJobExecution(models.Model):
    SENT = "开始执行"
    SUCCESS = "正在执行"
    MISSED = "任务丢失"
    MAX_INSTANCES = "实例阻塞"
    ERROR = "任务错误"

    STATUS_CHOICES = [
        (x, x)
        for x in [
            SENT,
            ERROR,
            SUCCESS,
        ]
    ]

    id = models.BigAutoField(
        primary_key=True, help_text=_("此作业的唯一ID。")
    )

    job = models.ForeignKey(
        DjangoJob,
        on_delete=models.CASCADE,
        help_text=_("与此执行相关的作业。"),
    )

    status = models.CharField(
        max_length=50,
        # TODO: Replace this with enumeration types when we drop support for Django 2.2
        # See: https://docs.djangoproject.com/en/dev/ref/models/fields/#field-choices-enum-types
        choices=STATUS_CHOICES,
        help_text=_("此作业执行的当前状态。"),
    )

    run_time = models.DateTimeField(
        db_index=True,
        help_text=_("执行此作业的日期和时间。"),
    )

    # We store this value in the DB even though it can be calculated as `finished - run_time`. This allows quick
    # calculation of average durations directly in the database later on.
    duration = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        default=None,
        null=True,
        help_text=_("此作业的总运行时间(秒)。"),
    )

    finished = models.DecimalField(
        max_digits=15,
        decimal_places=2,
        default=None,
        null=True,
        help_text=_("此作业完成的时间戳。"),
    )

    exception = models.CharField(
        max_length=1000,
        null=True,
        help_text=_(
            "作业执行期间发生的异常的详细信息(如果有)。"
        ),
    )

    traceback = models.TextField(
        null=True,
        help_text=_(
            "回溯作业执行期间发生的异常(如果有)。"
        ),
    )

    objects = DjangoJobExecutionManager()

    @classmethod
    @util.retry_on_db_operational_error
    def atomic_update_or_create(
        cls,
        lock,
        job_id: str,
        run_time: datetime,
        status: str,
        exception: str = None,
        traceback: str = None,
    ) -> "DjangoJobExecution":
        """
        使用APScheduler锁确保一次只能创建/更新一个数据库条目。

        这使Django_apScheduler与APScheduler保持同步，并维护APScheduler事件之间的1：1映射。
        以及持久保存到数据库的相应DjangoJobExecution模型实例。
        ：param lock：更新数据库时使用的锁-可能是通过调用_Scheduler._Create_lock()获得的。
        用法：param job_id：执行此作业的APScheduler作业的ID。
        ：param run_time：此作业执行的调度程序运行时。
        ：param Status：作业执行的新状态。
        ：param异常：需要记录的任何异常的详细信息。
        用法：param traceback：回溯执行作业时发生的任何异常。
        ：Return：新创建或更新的DjangoJobExecution的ID。
        """

        # Ensure that only one update / create can be processed at a time, staying in sync with corresponding
        # scheduler.
        with lock:
            # Convert all datetimes to internal Django format before doing calculations and persisting in the database.
            run_time = get_django_internal_datetime(run_time)

            finished = get_django_internal_datetime(timezone.now())
            duration = (finished - run_time).total_seconds()
            finished = finished.timestamp()

            try:
                with transaction.atomic():
                    job_execution = DjangoJobExecution.objects.select_for_update().get(
                        job_id=job_id, run_time=run_time
                    )

                    if status == DjangoJobExecution.SENT:
                        # Ignore 'submission' events for existing job executions. APScheduler does not appear to
                        # guarantee the order in which events are received, and it is not unusual to receive an
                        # `executed` before the corresponding `submitted` event. We just discard `submitted` events
                        # that are received after the job has already been executed.
                        #
                        # If there are any more instances like this then we probably want to implement a full blown
                        # state machine using something like `pytransitions`
                        # See https://github.com/pytransitions/transitions
                        return job_execution

                    job_execution.finished = finished
                    job_execution.duration = duration
                    job_execution.status = status

                    if exception:
                        job_execution.exception = exception

                    if traceback:
                        job_execution.traceback = traceback

                    job_execution.save()

            except DjangoJobExecution.DoesNotExist:
                # Execution was not created by a 'submit' previously - do so now
                if status == DjangoJobExecution.SENT:
                    # Don't log durations until after job has been submitted for execution
                    finished = None
                    duration = None

                job_execution = DjangoJobExecution.objects.create(
                    job_id=job_id,
                    run_time=run_time,
                    status=status,
                    duration=duration,
                    finished=finished,
                    exception=exception,
                    traceback=traceback,
                )

        return job_execution

    def __str__(self):
        return f"{self.id}: job '{self.job_id}' ({self.status})"

    class Meta:
        ordering = ("-run_time",)
        # unique_together = ("job_id", "run_time",)
        constraints = [
            UniqueConstraint(
                fields=["job_id", "run_time"], name="unique_job_executions"
            )
        ]
        verbose_name = "任务日志"
        verbose_name_plural = verbose_name


from apscheduler.triggers.cron import CronTrigger
from  .descovertasks import task_list,task_func

# 配置定时任务的模型
class TimedTaskConfig(models.Model):
    """
    定时任务
    """
    STATUS_CHOICES = (
        (0, "未启动"),
        (1, "已启动"),
    )
    FUNC_CHOICES = ( (t.name,t.docs) for t in task_list)
    id=models.BigAutoField(primary_key=True)
    task=models.CharField("任务",choices=FUNC_CHOICES, max_length=255)
    cron = models.CharField(max_length=100, verbose_name="cron表达式",
                            help_text="cron表达式，如：0 0 1 * * ?")
    # 定时任务执行的参数
    args=models.TextField("args",help_text="list格式,如:['1','2']",default="[]",null=True,blank=True)
    kwargs = models.TextField("kwargs",default="{}",
                                     help_text="json格式,如: {'a':1,'b':2}",null=True,blank=True)
    remark = models.CharField(max_length=200, verbose_name="备注")
    # 定时任务状态
    status = models.BooleanField(default=True, verbose_name="定时任务状态",
                                 help_text="定时任务状态")

    def save(self, *args, **kwargs):
        if not self.remark:
            self.remark = f"task_{self.pk}_{self.task}"
        try:
            from .run import scheduler
            if self.status:
                scheduler.add_job(
                    func=task_func(self.task),
                    args=json.loads(self.args),
                    kwargs=json.loads(self.kwargs),
                    trigger=CronTrigger.from_crontab(self.cron),
                    id=self.remark
                )
            else:
                try:
                    scheduler.remove_job(self.remark)
                except:
                    pass
            super().save(*args, **kwargs)
        except Exception as e:
            raise e
    class Meta:
        verbose_name = "任务配置"
        verbose_name_plural = verbose_name

    def __str__(self):
        return self.task