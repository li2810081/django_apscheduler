import time
from datetime import timedelta

from apscheduler import events
from apscheduler.schedulers.background import BackgroundScheduler
from django.conf import settings
from django.contrib import admin, messages
from django.db.models import Avg
from django.utils import timezone
from django.utils.html import format_html
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from django_apscheduler.models import DjangoJob, DjangoJobExecution,TimedTaskConfig
from django_apscheduler import util
from django_apscheduler.jobstores import DjangoJobStore, DjangoMemoryJobStore



@admin.register(DjangoJob)
class DjangoJobAdmin(admin.ModelAdmin):
    search_fields = ["id"]
    list_display = ["id", "local_run_time", "average_duration"]

    def __init__(self, model, admin_site):
        super().__init__(model, admin_site)

        self._django_jobstore = DjangoJobStore()
        self._memory_jobstore = DjangoMemoryJobStore()

        self._jobs_scheduled = None
        self._jobs_executed = None
        self._job_execution_timeout = getattr(
            settings, "APSCHEDULER_RUN_NOW_TIMEOUT", 15
        )

    def get_queryset(self, request):
        qs = super().get_queryset(request)

        self.avg_duration_qs = (
            DjangoJobExecution.objects.filter(
                job_id__in=qs.values_list("id", flat=True)
            )
            .order_by("job_id")
            .values_list("job")
            .annotate(avg_duration=Avg("duration"))
        )

        return qs

    def local_run_time(self, obj):
        if obj.next_run_time:
            return util.get_local_dt_format(obj.next_run_time)

        return "(暂停中)"

    def average_duration(self, obj):
        try:
            return self.avg_duration_qs.get(job_id=obj.id)[1]
        except DjangoJobExecution.DoesNotExist:
            return "无"

    average_duration.short_description = _("Average Duration (sec)")

    actions = ["run_selected_jobs"]

    def run_selected_jobs(self, request, queryset):
        scheduler = BackgroundScheduler()
        scheduler.add_jobstore(self._memory_jobstore)
        scheduler.add_listener(self._handle_execution_event, events.EVENT_JOB_EXECUTED)

        scheduler.start()

        self._jobs_scheduled = set()
        self._jobs_executed = set()
        start_time = timezone.now()

        for item in queryset:
            django_job = self._django_jobstore.lookup_job(item.id)

            if not django_job:
                msg = _("无法在数据库找到TASK {} ! 跳过执行...")
                self.message_user(request, format_html(msg, item.id), messages.WARNING)
                continue

            scheduler.add_job(
                django_job.func_ref,
                trigger=None,  # Run immediately
                args=django_job.args,
                kwargs=django_job.kwargs,
                id=django_job.id,
                name=django_job.name,
                misfire_grace_time=django_job.misfire_grace_time,
                coalesce=django_job.coalesce,
                max_instances=django_job.max_instances,
            )

            self._jobs_scheduled.add(django_job.id)

        while self._jobs_scheduled != self._jobs_executed:
            # Wait for selected jobs to be executed.
            if timezone.now() > start_time + timedelta(
                seconds=self._job_execution_timeout
            ):
                msg = _(
                    "最大运行时间超过 {} 秒! 并非所有作业都成功完成. "
                    "挂起的作业: {}"
                )
                self.message_user(
                    request,
                    format_html(
                        msg,
                        self._job_execution_timeout,
                        ",".join(self._jobs_scheduled - self._jobs_executed),
                    ),
                    messages.ERROR,
                )

                scheduler.shutdown(wait=False)
                return None

            time.sleep(0.1)

        for job_id in self._jobs_executed:
            self.message_user(request, format_html(_("已执行的作业 '{}'!"), job_id))

        scheduler.shutdown()
        return None

    def _handle_execution_event(self, event: events.JobExecutionEvent):
        self._jobs_executed.add(event.job_id)

    run_selected_jobs.short_description = _("执行选中的作业")


@admin.register(DjangoJobExecution)
class DjangoJobExecutionAdmin(admin.ModelAdmin):
    status_color_mapping = {
        DjangoJobExecution.SUCCESS: "green",
        DjangoJobExecution.SENT: "blue",
        DjangoJobExecution.MAX_INSTANCES: "orange",
        DjangoJobExecution.MISSED: "orange",
        DjangoJobExecution.ERROR: "red",
    }

    list_display = ["id", "job", "html_status", "local_run_time", "duration_text"]
    list_filter = ["job__id", "run_time", "status"]

    def html_status(self, obj):
        return mark_safe(
            f'<p style="color: {self.status_color_mapping[obj.status]}">{obj.status}</p>'
        )

    def local_run_time(self, obj):
        return util.get_local_dt_format(obj.run_time)

    def duration_text(self, obj):
        return obj.duration or "N/A"

    html_status.short_description = _("状态")
    duration_text.short_description = _("持续时间 (秒)")



@admin.register(TimedTaskConfig)
class TimedTaskConfigAdmin(admin.ModelAdmin):
    '''Admin View for TimedTaskConfig'''

    list_display = ('remark','task',"status","next_run_time")
    list_editable=('status',)
    list_filter = ('status',)

    def next_run_time(self, obj):
        try:
            obj=DjangoJob.objects.get(id=obj.remark)
            return obj
        except DjangoJob.DoesNotExist:
            return "无计划"

    next_run_time.short_description = '下次运行时间'
