"""Alert notification dispatcher."""

import asyncio
from datetime import datetime
from html import escape

from iot_backend.database import SessionLocal
from iot_backend.models import Alert, IoTDevice, User, UserNotificationTarget
from iot_backend.services.email_service import send_email_alert
from iot_backend.services.telegram_service import send_telegram_message


_METRIC_LABELS = {
    "temperature": "Temperature",
    "humidity": "Humidity",
    "soil_moisture": "Soil moisture",
    "light_intensity": "Light intensity",
    "pressure": "Pressure",
}


def _metric_label(metric_type: str | None) -> str:
    metric_key = str(metric_type or "").strip().lower()
    return _METRIC_LABELS.get(metric_key, metric_key or "Unknown")


def _device_label(alert: Alert, device: IoTDevice) -> str:
    return str(alert.device_name or device.name or alert.source or "Unknown").strip()


def _unit(alert: Alert, device: IoTDevice) -> str:
    return str(alert.unit or device.unit or "").strip()


def _format_value(value: float | None, *, unit: str = "") -> str:
    if value is None:
        return "--"
    text = f"{float(value):.2f}"
    return f"{text} {unit}".strip() if unit else text


def _threshold_text(alert: Alert, device: IoTDevice, html: bool = False) -> str:
    op = ">" if alert.status == "critical" else "<"
    if html:
        op = "&gt;" if op == ">" else "&lt;"
    return f"{op} {_format_value(alert.threshold, unit=_unit(alert, device))}"


def _allowed_range_text(alert: Alert, device: IoTDevice, *, html: bool = False) -> str:
    min_threshold = alert.min_threshold
    max_threshold = alert.max_threshold
    unit = _unit(alert, device)
    if min_threshold is None and max_threshold is None:
        return "--"
    if min_threshold is None:
        range_text = f"<= {_format_value(max_threshold, unit=unit)}"
    elif max_threshold is None:
        range_text = f">= {_format_value(min_threshold, unit=unit)}"
    else:
        range_text = (
            f"{_format_value(min_threshold, unit=unit)} - "
            f"{_format_value(max_threshold, unit=unit)}"
        )
    return escape(range_text) if html else range_text


def _status_label(alert: Alert) -> str:
    return "CRITICAL" if alert.status == "critical" else "WARNING"


def _alert_time(alert: Alert) -> str:
    value = alert.created_at or datetime.now()
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _is_forecast_alert(alert: Alert) -> bool:
    return str(getattr(alert, "alert_origin", "realtime") or "realtime").strip().lower() == "forecast"


def _title(alert: Alert) -> str:
    return "MetricsPulse Forecast Alert" if _is_forecast_alert(alert) else "MetricsPulse IoT Alert"


def _value_label(alert: Alert) -> str:
    return "Forecast value" if _is_forecast_alert(alert) else "Current value"


def _forecast_time(alert: Alert) -> str | None:
    value = getattr(alert, "forecast_timestamp", None)
    if not value:
        return None
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _forecast_generated_time(alert: Alert) -> str | None:
    value = getattr(alert, "forecast_generated_at", None)
    if not value:
        return None
    return value.strftime("%Y-%m-%d %H:%M:%S")


def build_alert_text(alert: Alert, device: IoTDevice) -> str:
    device_label = _device_label(alert, device)
    source_label = str(alert.source or "Unknown")
    metric_label = _metric_label(alert.metric_type)
    current_value = _format_value(alert.current_value, unit=_unit(alert, device))
    threshold_text = _threshold_text(alert, device)
    allowed_range = _allowed_range_text(alert, device)
    status_label = _status_label(alert)
    alert_time = _alert_time(alert)
    lines = [
        f"{_title(alert)}\n",
        f"Device: {device_label}\n"
        f"Source: {source_label}\n"
        f"Metric: {metric_label}\n"
        f"{_value_label(alert)}: {current_value}\n"
        f"Threshold trigger: {threshold_text}\n"
        f"Allowed range: {allowed_range}\n"
        f"Status: {status_label}\n"
        f"Time: {alert_time}",
    ]
    forecast_time = _forecast_time(alert)
    if forecast_time:
        lines.append(f"\nForecast time: {forecast_time}")
    forecast_generated_time = _forecast_generated_time(alert)
    if forecast_generated_time:
        lines.append(f"\nForecast generated at: {forecast_generated_time}")
    return "".join(lines)


def _build_telegram_message(alert: Alert, device: IoTDevice) -> str:
    device_label = escape(_device_label(alert, device))
    source_label = escape(str(alert.source or "Unknown"))
    metric_label = escape(_metric_label(alert.metric_type))
    current_value = escape(_format_value(alert.current_value, unit=_unit(alert, device)))
    threshold_text = _threshold_text(alert, device, html=True)
    allowed_range = _allowed_range_text(alert, device, html=True)
    status_label = escape(_status_label(alert))
    alert_time = escape(_alert_time(alert))
    message = (
        f"<b>{escape(_title(alert))}</b>\n"
        f"<b>Device:</b> {device_label}\n"
        f"<b>Source:</b> {source_label}\n"
        f"<b>Metric:</b> {metric_label}\n"
        f"<b>{escape(_value_label(alert))}:</b> {current_value}\n"
        f"<b>Threshold trigger:</b> {threshold_text}\n"
        f"<b>Allowed range:</b> {allowed_range}\n"
        f"<b>Status:</b> {status_label}\n"
        f"<b>Time:</b> {alert_time}"
    )
    forecast_time = _forecast_time(alert)
    if forecast_time:
        message += f"\n<b>Forecast time:</b> {escape(forecast_time)}"
    forecast_generated_time = _forecast_generated_time(alert)
    if forecast_generated_time:
        message += f"\n<b>Forecast generated at:</b> {escape(forecast_generated_time)}"
    return message


def _build_email_html(alert: Alert, device: IoTDevice) -> str:
    device_label = escape(_device_label(alert, device))
    source_label = escape(str(alert.source or "Unknown"))
    metric_label = escape(_metric_label(alert.metric_type))
    current_value = escape(_format_value(alert.current_value, unit=_unit(alert, device)))
    threshold_text = _threshold_text(alert, device, html=True)
    allowed_range = _allowed_range_text(alert, device, html=True)
    status_label = escape(_status_label(alert))
    alert_time = escape(_alert_time(alert))
    forecast_time = _forecast_time(alert)
    forecast_generated_time = _forecast_generated_time(alert)
    extra_html = ""
    if forecast_time:
        extra_html += f"<p><b>Forecast time:</b> {escape(forecast_time)}</p>"
    if forecast_generated_time:
        extra_html += f"<p><b>Forecast generated at:</b> {escape(forecast_generated_time)}</p>"
    return f"""
    <html>
      <body>
        <h2>{escape(_title(alert))}</h2>
        <p><b>Device:</b> {device_label}</p>
        <p><b>Source:</b> {source_label}</p>
        <p><b>Metric:</b> {metric_label}</p>
        <p><b>{escape(_value_label(alert))}:</b> {current_value}</p>
        <p><b>Threshold trigger:</b> {threshold_text}</p>
        <p><b>Allowed range:</b> {allowed_range}</p>
        <p><b>Status:</b> {status_label}</p>
        <p><b>Time:</b> {alert_time}</p>
        {extra_html}
      </body>
    </html>
    """


def _build_email_subject(alert: Alert, device: IoTDevice) -> str:
    prefix = "Forecast" if _is_forecast_alert(alert) else "Alert"
    return f"[MetricsPulse {prefix}] {_metric_label(alert.metric_type)} out of range on {_device_label(alert, device)}"


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
            tasks.append(asyncio.to_thread(send_email_alert, email, _build_email_subject(alert, device), _build_email_html(alert, device)))

        if not telegram_targets and owner.telegram_enabled and owner.telegram_chat_id:
            print("[ALERT] Sending Telegram")
            tasks.append(asyncio.to_thread(send_telegram_message, owner.telegram_chat_id, _build_telegram_message(alert, device)))

        if not email_targets:
            destination_email = owner.notification_email or owner.email
            if owner.email_enabled and destination_email:
                print("[ALERT] Sending Gmail")
                tasks.append(asyncio.to_thread(send_email_alert, destination_email, _build_email_subject(alert, device), _build_email_html(alert, device)))

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
