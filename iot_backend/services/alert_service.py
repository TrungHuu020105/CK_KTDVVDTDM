"""Alert notification dispatcher."""

import asyncio
from datetime import datetime

from iot_backend.database import SessionLocal
from iot_backend.models import Alert, IoTDevice, User, UserNotificationTarget
from iot_backend.services.email_service import send_email_alert
from iot_backend.services.telegram_service import send_telegram_message


def _threshold_text(alert: Alert, html: bool = False) -> str:
    op = ">" if alert.status == "critical" else "<"
    if html and op == ">":
        op = "&gt;"
    if html and op == "<":
        op = "&lt;"
    return f"{op} {alert.threshold}"


def _alert_time(alert: Alert) -> str:
    value = alert.created_at or datetime.now()
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _build_telegram_message(alert: Alert, device: IoTDevice) -> str:
    return (
        "IoT Alert\n"
        f"Device: {alert.source}\n"
        f"Metric: {alert.metric_type}\n"
        f"Current value: {alert.current_value}\n"
        f"Threshold: {_threshold_text(alert)}\n"
        f"Time: {_alert_time(alert)}\n"
        "Source: metrics/live ESP32"
    )


def _build_email_html(alert: Alert, device: IoTDevice) -> str:
    return f"""
    <html>
      <body>
        <h2>IoT Alert</h2>
        <p><b>Device:</b> {alert.source}</p>
        <p><b>Metric:</b> {alert.metric_type}</p>
        <p><b>Current value:</b> {alert.current_value}</p>
        <p><b>Threshold:</b> {_threshold_text(alert, html=True)}</p>
        <p><b>Time:</b> {_alert_time(alert)}</p>
        <p><b>Source:</b> metrics/live ESP32</p>
      </body>
    </html>
    """


async def dispatch_alert_notifications(alert_id: int):
    """Dispatch Telegram and email notifications asynchronously for device owner."""
    db = SessionLocal()
    try:
        alert = db.query(Alert).filter(Alert.id == alert_id).first()
        if not alert:
            print(f"[NOTIFY] Alert {alert_id} not found")
            return
        device = db.query(IoTDevice).filter(IoTDevice.source == alert.source).first()
        if not device:
            print(f"[NOTIFY] No device found for source={alert.source}")
            return
        owner = db.query(User).filter(User.id == device.user_id).first()
        if not owner:
            print(f"[NOTIFY] No owner found for device_id={device.id}")
            return

        tasks = []
        targets = db.query(UserNotificationTarget).filter(
            UserNotificationTarget.user_id == owner.id,
            UserNotificationTarget.is_enabled == True,
        ).all()

        telegram_targets = [t.target_value for t in targets if t.target_type == "telegram"]
        email_targets = [t.target_value for t in targets if t.target_type == "email"]

        for chat_id in telegram_targets:
            print("[ALERT] Sending Telegram")
            tasks.append(asyncio.to_thread(send_telegram_message, chat_id, _build_telegram_message(alert, device)))

        for email in email_targets:
            print("[ALERT] Sending Gmail")
            tasks.append(asyncio.to_thread(send_email_alert, email, f"[IoT Alert] {alert.metric_type} on {alert.source}", _build_email_html(alert, device)))

        if not telegram_targets and owner.telegram_enabled and owner.telegram_chat_id:
            print("[ALERT] Sending Telegram")
            tasks.append(asyncio.to_thread(send_telegram_message, owner.telegram_chat_id, _build_telegram_message(alert, device)))

        if not email_targets:
            destination_email = owner.notification_email or owner.email
            if owner.email_enabled and destination_email:
                print("[ALERT] Sending Gmail")
                tasks.append(asyncio.to_thread(send_email_alert, destination_email, f"[IoT Alert] {alert.metric_type} on {alert.source}", _build_email_html(alert, device)))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            print(
                f"[NOTIFY] Dispatch results for alert_id={alert_id} "
                f"(user_id={owner.id}, telegram_targets={len(telegram_targets)}, email_targets={len(email_targets)}, "
                f"email_enabled={owner.email_enabled}, fallback_email={owner.notification_email or owner.email}): {results}"
            )
        else:
            print(f"[NOTIFY] No channels enabled for user_id={owner.id}")
    finally:
        db.close()
