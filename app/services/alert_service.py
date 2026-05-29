"""Alert notification dispatcher."""

import asyncio
from datetime import datetime

from app.database import SessionLocal
from app.models import Alert, IoTDevice, User, UserNotificationTarget
from app.services.email_service import send_email_alert
from app.services.telegram_service import send_telegram_message


def _alert_time(alert: Alert) -> str:
    value = alert.created_at or datetime.now()
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _thresholds(device: IoTDevice) -> tuple[float | None, float | None]:
    return (
        device.min_threshold,
        device.max_threshold,
    )


def _current_value(alert: Alert, device: IoTDevice) -> str:
    unit = device.unit or ""
    return f"{alert.current_value:.2f} {unit}".strip()


def _build_message_body(alert: Alert, device: IoTDevice) -> str:
    min_threshold, max_threshold = _thresholds(device)
    device_name = device.name or alert.source
    return (
        f"Device: {device_name}\n"
        f"Source: {alert.source}\n"
        f"Metric: {alert.metric_type}\n"
        f"Current value: {_current_value(alert, device)}\n"
        f"Threshold: min {min_threshold}, max {max_threshold}\n"
        "Status: OUT OF RANGE\n"
        f"Time: {_alert_time(alert)}"
    )


def _build_email_subject(alert: Alert, device: IoTDevice) -> str:
    device_name = device.name or alert.source
    return f"[MetricsPulse Alert] {device_name} {alert.metric_type} OUT OF RANGE"


def _build_telegram_message(alert: Alert, device: IoTDevice) -> str:
    return "[MetricsPulse Alert] OUT OF RANGE\n" + _build_message_body(alert, device)


def _send_email_alert_logged(email: str, subject: str, body: str):
    print(f"[EMAIL ALERT] sending to {email}")
    ok, detail = send_email_alert(email, subject, body)
    if ok:
        print(f"[EMAIL ALERT] sent to {email}")
    else:
        print(f"[EMAIL ALERT] failed to send to {email}: {detail}")
    return ok, detail


async def send_email_alert_to_enabled_recipients(db, alert: Alert, device: IoTDevice, owner: User):
    """Send alert email to every enabled email notification target for the owner."""
    email_targets = [
        t.target_value
        for t in db.query(UserNotificationTarget).filter(
            UserNotificationTarget.user_id == owner.id,
            UserNotificationTarget.target_type == "email",
            UserNotificationTarget.is_enabled == True,
        ).all()
    ]
    print(f"[EMAIL ALERT] enabled recipients count: {len(email_targets)}")
    if not email_targets:
        return []

    subject = _build_email_subject(alert, device)
    body = _build_message_body(alert, device)
    tasks = [
        asyncio.to_thread(_send_email_alert_logged, email, subject, body)
        for email in email_targets
    ]
    return await asyncio.gather(*tasks, return_exceptions=True)


async def dispatch_alert_notifications(alert_id: int):
    """Dispatch Telegram and email notifications asynchronously for device owner."""
    db = SessionLocal()
    try:
        alert = db.query(Alert).filter(Alert.id == alert_id).first()
        if not alert:
            print(f"[NOTIFY] Alert {alert_id} not found")
            return

        device = db.query(IoTDevice).filter(
            IoTDevice.source == alert.source,
            IoTDevice.device_type == alert.metric_type,
        ).first()
        if not device:
            print(f"[NOTIFY] No device found for source={alert.source} metric_type={alert.metric_type}")
            return

        min_threshold, max_threshold = _thresholds(device)
        if not device.is_active or not device.alert_enabled or min_threshold is None or max_threshold is None:
            print(f"[NOTIFY] Skip alert_id={alert_id}: thresholds not fully configured")
            return

        owner = db.query(User).filter(User.id == device.user_id).first()
        if not owner:
            print(f"[NOTIFY] No owner found for device_id={device.id}")
            return

        targets = db.query(UserNotificationTarget).filter(
            UserNotificationTarget.user_id == owner.id,
            UserNotificationTarget.is_enabled == True,
        ).all()

        telegram_targets = [t.target_value for t in targets if t.target_type == "telegram"]

        tasks = []
        for chat_id in telegram_targets:
            print("[ALERT] Sending Telegram")
            tasks.append(asyncio.to_thread(send_telegram_message, chat_id, _build_telegram_message(alert, device)))

        email_results = await send_email_alert_to_enabled_recipients(db, alert, device, owner)

        if not telegram_targets and owner.telegram_enabled and owner.telegram_chat_id:
            print("[ALERT] Sending Telegram")
            tasks.append(asyncio.to_thread(send_telegram_message, owner.telegram_chat_id, _build_telegram_message(alert, device)))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            print(
                f"[NOTIFY] Dispatch results for alert_id={alert_id} "
                f"(user_id={owner.id}, telegram_targets={len(telegram_targets)}, "
                f"email_results={email_results}): {results}"
            )
        elif email_results:
            print(f"[NOTIFY] Dispatch email results for alert_id={alert_id}: {email_results}")
        else:
            print(f"[NOTIFY] No channels enabled for user_id={owner.id}")
    finally:
        db.close()
